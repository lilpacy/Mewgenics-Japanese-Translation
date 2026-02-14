"""日本語テキスト自動改行スクリプト。

CSVファイルの ja 列を読み込み、表示上15文字以上のテキストに対して
15文字ごとに改行（LF）を挿入する。タグの途中では改行しない。
"""

import argparse
import csv
import os
import re
from typing import List, Tuple

# [tag:param] or [/tag] — formatting/control tags (zero display width)
TAG_PATTERN = re.compile(r"\[/?[^\[\]]+\]")
# [img:xxx] — rendered as a single icon (display width = 1)
IMG_TAG_PATTERN = re.compile(r"\[img:\w+\]")
# HTML entities like &nbsp; (display width = 1)
ENTITY_PATTERN = re.compile(r"&\w+;")

REPORT_FILES = {"m_newline_scan_report.csv", "missing_translation_report.csv"}


def _tokenize_plain(text: str) -> List[Tuple[str, int]]:
    """プレーンテキスト部分をトークンに分割する。

    連続するASCII英数字（英単語・数値）はまとめて1トークンにし、
    途中で改行されないようにする。表示幅は文字数分。
    """
    tokens: List[Tuple[str, int]] = []
    ascii_word = re.compile(r"[A-Za-z0-9]+")
    pos = 0
    for m in ascii_word.finditer(text):
        for ch in text[pos:m.start()]:
            tokens.append((ch, 1))
        word = m.group(0)
        tokens.append((word, len(word)))
        pos = m.end()
    for ch in text[pos:]:
        tokens.append((ch, 1))
    return tokens


def tokenize(text: str) -> List[Tuple[str, int]]:
    """テキストをトークンに分割する。

    各トークンは (文字列, 表示幅) のタプル。
    - [img:xxx] タグ: 表示幅1
    - その他のタグ [m:happy], [b], [/b] 等: 表示幅0
    - {variable} テンプレート変数: 表示幅=中の文字数
    - &nbsp; 等のHTMLエンティティ: 表示幅1
    - 連続するASCII英数字（英単語）: 表示幅=文字数（分割不可）
    - 通常の文字: 表示幅1
    """
    tokens: List[Tuple[str, int]] = []
    combined = re.compile(
        r"(\[img:\w+\])"       # group 1: img tag (width=1)
        r"|(\[/?[^\[\]]+\])"   # group 2: other tags (width=0)
        r"|(\{[^{}]+\})"       # group 3: template variable e.g. {catname}
        r"|(&\w+;)"            # group 4: HTML entity (width=1)
    )

    pos = 0
    for m in combined.finditer(text):
        # Plain text before this match — split into words/chars
        if m.start() > pos:
            tokens.extend(_tokenize_plain(text[pos:m.start()]))

        if m.group(1):    # [img:xxx]
            tokens.append((m.group(1), 1))
        elif m.group(2):  # other tag
            tokens.append((m.group(2), 0))
        elif m.group(3):  # {variable}
            var = m.group(3)
            tokens.append((var, len(var)))
        elif m.group(4):  # &entity;
            tokens.append((m.group(4), 1))

        pos = m.end()

    # Remaining plain text
    if pos < len(text):
        tokens.extend(_tokenize_plain(text[pos:]))

    return tokens


def wrap_segment(segment: str, max_len: int) -> str:
    """1行分のテキストを max_len 表示文字ごとに改行する。"""
    tokens = tokenize(segment)

    # Calculate total display length
    total_display = sum(w for _, w in tokens)
    if total_display <= max_len:
        return segment

    lines: List[str] = []
    current = ""
    current_display = 0

    for token_str, token_width in tokens:
        if token_width > 0 and current_display + token_width > max_len and current:
            lines.append(current)
            current = token_str
            current_display = token_width
        else:
            current += token_str
            current_display += token_width

    if current:
        lines.append(current)

    return "\n".join(lines)


def wrap_ja_text(text: str, max_len: int) -> Tuple[str, bool]:
    """ja列のテキスト全体を処理する。既存の改行を保持しつつ各行を折り返す。"""
    if not text:
        return text, False

    parts = text.split("\n")
    wrapped_parts = [wrap_segment(part, max_len) for part in parts]
    wrapped = "\n".join(wrapped_parts)
    return wrapped, wrapped != text


def process_file(filepath: str, max_len: int, dry_run: bool) -> Tuple[int, int]:
    """1つのCSVファイルを処理する。変更行数と追加改行数を返す。"""
    with open(filepath, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    if "ja" not in fieldnames:
        return 0, 0

    rows_changed = 0
    wraps_added = 0

    for row in rows:
        key = (row.get("KEY") or "").strip()
        if not key or key.startswith("//"):
            continue

        original = row.get("ja")
        if not original:
            continue

        wrapped, changed = wrap_ja_text(original, max_len)
        if changed:
            if dry_run:
                print(f"  [{key}]")
                print(f"    before: {repr(original)}")
                print(f"    after:  {repr(wrapped)}")
            row["ja"] = wrapped
            rows_changed += 1
            wraps_added += wrapped.count("\n") - original.count("\n")

    if not dry_run and rows_changed > 0:
        with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    return rows_changed, wraps_added


def main() -> None:
    parser = argparse.ArgumentParser(
        description="日本語テキスト(ja列)が長い場合に自動改行を挿入する。"
    )
    parser.add_argument("input_dir", help="CSVファイルが格納されたディレクトリ")
    parser.add_argument(
        "--max-len",
        type=int,
        default=15,
        help="1行あたりの最大表示文字数（デフォルト: 15）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="変更内容をプレビューのみ表示し、ファイルを書き換えない",
    )
    args = parser.parse_args()

    input_dir = os.path.abspath(args.input_dir)
    if not os.path.isdir(input_dir):
        raise SystemExit(f"ディレクトリが見つかりません: {input_dir}")

    csv_files = sorted(
        f
        for f in os.listdir(input_dir)
        if f.endswith(".csv") and f not in REPORT_FILES
    )

    if args.dry_run:
        print("[DRY RUN] ファイルは変更されません\n")

    print(f"対象ディレクトリ: {input_dir}")
    print(f"最大表示文字数: {args.max_len}")
    print(f"対象ファイル数: {len(csv_files)}")
    print("-" * 48)

    total_rows = 0
    total_wraps = 0

    for name in csv_files:
        filepath = os.path.join(input_dir, name)
        rows_changed, wraps_added = process_file(filepath, args.max_len, args.dry_run)
        total_rows += rows_changed
        total_wraps += wraps_added
        status = f"rows: {rows_changed}, wraps: +{wraps_added}" if rows_changed else "変更なし"
        print(f"{name}: {status}")

    print("-" * 48)
    print(f"変更行数合計: {total_rows}")
    print(f"追加改行数合計: {total_wraps}")


if __name__ == "__main__":
    main()
