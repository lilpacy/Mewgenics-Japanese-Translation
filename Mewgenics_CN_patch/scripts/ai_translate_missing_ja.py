import argparse
import concurrent.futures
import csv
import json
import os
import re
import socket
import time
import urllib.error
import urllib.request
from typing import Dict, List, Optional, Tuple

OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"

TAG_OR_VAR_PATTERN = re.compile(r"\[img:[^\]]+\]|\[[^\]]+\]|\{[^{}]*\}|&nbsp;", re.IGNORECASE)
ALNUM_PATTERN = re.compile(r"[A-Za-z0-9\u00C0-\u024F\u4E00-\u9FFF]")

SKIP_FILES = {
    "missing_translation_report.csv",
    "m_newline_scan_report.csv",
}


def normalize_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    return value.strip()


def is_symbolic_or_tag_only(text: str) -> bool:
    """タグ・変数・記号のみで実質的な翻訳対象テキストがない場合 True を返す。"""
    normalized = normalize_text(text)
    if normalized == "":
        return False
    stripped = TAG_OR_VAR_PATTERN.sub("", normalized)
    stripped = stripped.replace("\\n", "").replace("\n", "").strip()
    return ALNUM_PATTERN.search(stripped) is None


def is_missing_translation(en_text: str, target_text: str) -> bool:
    en_norm = normalize_text(en_text)
    target_norm = normalize_text(target_text)
    if en_norm == "":
        return False
    return target_norm == "" or target_norm == en_norm


def load_targets_from_report(report_path: str, target_col_name: str) -> Dict[str, set]:
    targets: Dict[str, set] = {}
    if not os.path.isfile(report_path):
        return targets

    with open(report_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            file_name = normalize_text(row.get("file"))
            key = normalize_text(row.get("key"))
            target_col = normalize_text(row.get("target_col"))
            if not file_name or not key:
                continue
            if target_col and target_col != target_col_name:
                continue

            if file_name not in targets:
                targets[file_name] = set()
            targets[file_name].add(key)

    return targets


def should_skip_row(row: Dict[str, str]) -> bool:
    key = normalize_text(row.get("KEY"))
    if key.startswith("//"):
        return True
    return all(normalize_text(v) == "" for v in row.values())


def build_key_index(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    index: Dict[str, Dict[str, str]] = {}
    for row in rows:
        key = normalize_text(row.get("KEY"))
        if key:
            index[key] = row
    return index


def get_desc_context(
    row_key: str, key_index: Dict[str, Dict[str, str]], target_col: str
) -> Tuple[str, str]:
    if not row_key.endswith("_NAME"):
        return "", ""

    base = row_key[: -len("_NAME")]
    desc_key = f"{base}_DESC"
    desc_row = key_index.get(desc_key)
    if not desc_row:
        return "", ""

    en_desc = desc_row.get("en") or ""
    target_desc = desc_row.get(target_col) or ""
    return en_desc, target_desc


def call_openai_chat(
    api_key: str, model: str, system_prompt: str, user_prompt: str, timeout_sec: int = 90
) -> str:
    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }

    req = urllib.request.Request(
        OPENAI_CHAT_COMPLETIONS_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        body = resp.read().decode("utf-8")
        data = json.loads(body)

    choices = data.get("choices") or []
    if not choices:
        return ""

    message = choices[0].get("message") or {}
    content = message.get("content") or ""
    return content.strip()


def sanitize_model_output(text: str) -> str:
    s = text.strip()
    s = re.sub(r"^```[a-zA-Z]*\n", "", s)
    s = re.sub(r"\n```$", "", s)
    return s.strip()


def translate_text(
    api_key: str,
    model: str,
    key: str,
    en_text: str,
    row_type: str,
    en_desc: str,
    target_desc: str,
    retries: int,
    sleep_sec: float,
) -> str:
    system_prompt = (
        "あなたはゲームローカライズ翻訳アシスタントです。"
        "ターゲット言語は日本語です。"
        "翻訳結果のみを出力してください。説明、引用符、余計な内容は出力しないでください。"
        "原文中のプレースホルダーやタグ形式を保持してください。例: {var}、[img:...]、[b]...[/b]。"
        "改行構造と意味の自然さを保ち、機械的な直訳を避けてください。"
    )

    if row_type == "name":
        user_prompt = (
            "ゲームの名前フィールドを翻訳してください。\n"
            "ルール：\n"
            "1) 日本語の短い名前を出力してください。\n"
            "2) 英語名が造語や辞書にない単語の場合（例: fartoom のような語）、"
            "説明文の文脈を参考に、可読性とゲームプレイの意味を優先して命名してください。\n"
            "3) 固有名詞として保持すべき場合を除き、英語をそのまま残さないでください。"
            "ただしカタカナ表記が自然な場合はカタカナを使用してください。\n"
            f"KEY: {key}\n"
            f"EN_NAME: {en_text}\n"
            f"EN_DESC_CONTEXT: {en_desc}\n"
            f"JA_DESC_CONTEXT: {target_desc}\n"
            "日本語の名前のみを返してください。"
        )
    else:
        user_prompt = (
            "ゲームの説明フィールドを日本語に翻訳してください。\n"
            "ルール：\n"
            "1) 正確かつ自然に、ゲーム用語に合わせて翻訳してください。\n"
            "2) プレースホルダーやタグを保持し、削除や書き換えをしないでください。\n"
            "3) 原文に改行がある場合、適切な改行を保持してください。\n"
            f"KEY: {key}\n"
            f"EN_DESC: {en_text}\n"
            "日本語の説明のみを返してください。"
        )

    last_error = None
    for attempt in range(retries + 1):
        try:
            result = call_openai_chat(api_key, model, system_prompt, user_prompt)
            result = sanitize_model_output(result)
            if result:
                return result
        except urllib.error.HTTPError as e:
            last_error = f"HTTP {e.code}"
            try:
                err_body = e.read().decode("utf-8", errors="ignore")
                if err_body:
                    last_error = f"{last_error}: {err_body[:300]}"
            except Exception:
                pass
        except Exception as e:
            last_error = str(e)

        if attempt < retries:
            time.sleep(sleep_sec)

    raise RuntimeError(last_error or "unknown api error")


def process_file(
    file_path: str,
    output_path: str,
    api_key: str,
    model: str,
    target_col: str,
    retries: int,
    sleep_sec: float,
    max_rows: int,
    report_targets: Optional[set],
) -> Tuple[int, int]:
    translated_rows = 0
    failed_rows = 0

    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    if "en" not in fieldnames or target_col not in fieldnames:
        with open(output_path, "w", encoding="utf-8-sig", newline="") as out_f:
            writer = csv.DictWriter(out_f, fieldnames=fieldnames)
            if fieldnames:
                writer.writeheader()
            writer.writerows(rows)
        return 0, 0

    key_index = build_key_index(rows)

    with open(output_path, "w", encoding="utf-8-sig", newline="") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()

        for row in rows:
            if should_skip_row(row):
                writer.writerow(row)
                continue

            key = normalize_text(row.get("KEY"))
            en_text = row.get("en") or ""
            target_text = row.get(target_col) or ""

            if report_targets is not None and key not in report_targets:
                writer.writerow(row)
                continue

            if is_symbolic_or_tag_only(en_text):
                writer.writerow(row)
                continue

            need_translate = is_missing_translation(en_text, target_text)
            if need_translate and (max_rows <= 0 or translated_rows < max_rows):
                row_type = (
                    "name" if key.endswith("_NAME")
                    else "desc" if key.endswith("_DESC")
                    else "generic"
                )
                en_desc_ctx, target_desc_ctx = get_desc_context(key, key_index, target_col)

                try:
                    translated = translate_text(
                        api_key=api_key,
                        model=model,
                        key=key,
                        en_text=en_text,
                        row_type=row_type,
                        en_desc=en_desc_ctx,
                        target_desc=target_desc_ctx,
                        retries=retries,
                        sleep_sec=sleep_sec,
                    )
                    if translated:
                        # 翻訳結果が入力の3倍以上長い場合はモデルの拒否/暴走とみなして破棄
                        if len(translated) > max(len(en_text) * 3, 100):
                            print(f"  [{key}] SKIPPED (output too long): {translated[:80]}")
                        else:
                            row[target_col] = translated
                            translated_rows += 1
                            print(f"  [{key}] {en_text[:60]} -> {translated[:60]}")
                except Exception as e:
                    failed_rows += 1
                    print(f"  [{key}] FAILED: {e}")

            writer.writerow(row)

    return translated_rows, failed_rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="OpenAI API で CSV の未翻訳項目を日本語に自動翻訳し、新ディレクトリに出力する。"
    )
    parser.add_argument("input_dir", help="CSV が格納されたディレクトリ")
    parser.add_argument(
        "--output-dir",
        default="ai_translated_output",
        help="出力サブディレクトリ（デフォルト: ai_translated_output）",
    )
    parser.add_argument(
        "--model",
        default="gpt-4o",
        help="OpenAI モデル名（デフォルト: gpt-4o）",
    )
    parser.add_argument(
        "--api-key",
        default="",
        help="OpenAI API Key（環境変数 OPENAI_API_KEY を推奨）",
    )
    parser.add_argument(
        "--target-col",
        default="ja",
        help="ターゲット言語の列名（デフォルト: ja）",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=2,
        help="失敗時のリトライ回数（デフォルト: 2）",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="リクエスト間隔（秒）（デフォルト: 0.5）",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="並列ワーカー数（0 = os.cpu_count() or 8）",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="最大翻訳行数（0 = 無制限）",
    )
    parser.add_argument(
        "--report",
        default="missing_translation_report.csv",
        help="漏訳レポートのパス（デフォルト: input_dir/missing_translation_report.csv）",
    )
    parser.add_argument(
        "--scan-all",
        action="store_true",
        help="レポートを使わず、ターゲット列が空または英語と同一の行を全て翻訳する",
    )
    args = parser.parse_args()

    api_key = (args.api_key or os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("API Key が必要です。--api-key または環境変数 OPENAI_API_KEY を設定してください。")

    input_dir = os.path.abspath(args.input_dir)
    if not os.path.isdir(input_dir):
        raise SystemExit(f"入力ディレクトリが存在しません: {input_dir}")

    output_dir = os.path.join(input_dir, args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    report_path = args.report
    if not os.path.isabs(report_path):
        report_path = os.path.join(input_dir, report_path)
    if args.scan_all:
        report_targets_by_file = None
        print("--scan-all: レポートを使わず、未翻訳行を全てスキャンします")
    else:
        report_targets_by_file = load_targets_from_report(report_path, args.target_col)
        if not report_targets_by_file:
            print(f"WARNING: 漏訳レポートが空または存在しません: {report_path}")
            print("--scan-all モードにフォールバックします")
            report_targets_by_file = None

    csv_files = sorted(
        name
        for name in os.listdir(input_dir)
        if name.lower().endswith(".csv")
        and name not in SKIP_FILES
        and os.path.isfile(os.path.join(input_dir, name))
    )

    max_workers = args.workers if args.workers > 0 else (os.cpu_count() or 8)
    max_workers = min(max_workers, len(csv_files) or 1)

    # グローバルソケットタイムアウト（urllib の timeout を補完）
    socket.setdefaulttimeout(120)

    print(f"並列ワーカー数: {max_workers}, 対象ファイル数: {len(csv_files)}")

    total_translated = 0
    total_failed = 0

    def _process_one(name: str) -> Tuple[str, int, int]:
        src = os.path.join(input_dir, name)
        dst = os.path.join(output_dir, name)
        report_targets = report_targets_by_file.get(name) if report_targets_by_file is not None else None

        translated, failed = process_file(
            file_path=src,
            output_path=dst,
            api_key=api_key,
            model=args.model,
            target_col=args.target_col,
            retries=max(0, args.retries),
            sleep_sec=max(0.0, args.sleep),
            max_rows=args.max_rows,
            report_targets=report_targets,
        )
        print(f"{name}: translated {translated}, failed {failed}")
        return name, translated, failed

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process_one, name): name for name in csv_files}
        for future in concurrent.futures.as_completed(futures):
            name = futures[future]
            try:
                _, translated, failed = future.result()
                total_translated += translated
                total_failed += failed
            except Exception as e:
                print(f"{name}: EXCEPTION: {e}")
                total_failed += 1

    print("-" * 48)
    print(f"files processed: {len(csv_files)}")
    print(f"rows translated: {total_translated}")
    print(f"rows failed: {total_failed}")
    if not args.scan_all:
        print(f"report used: {report_path}")
    print(f"output dir: {output_dir}")


if __name__ == "__main__":
    main()
