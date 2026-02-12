import argparse
import csv
import os
from typing import Dict, Optional, Tuple


def normalize_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    return value.strip()


def merge_file(
    base_path: str,
    translated_path: str,
    output_path: str,
    target_col: str,
) -> Tuple[int, int]:
    """translated_path の target_col を base_path にマージし output_path に出力する。"""
    with open(translated_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        translated_fieldnames = reader.fieldnames or []
        translated_rows = list(reader)

    if "KEY" not in translated_fieldnames or target_col not in translated_fieldnames:
        return 0, 0

    translated_index: Dict[str, str] = {}
    for row in translated_rows:
        key = normalize_text(row.get("KEY"))
        value = row.get(target_col) or ""
        if key and normalize_text(value):
            translated_index[key] = value

    if not translated_index:
        return 0, 0

    with open(base_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        base_fieldnames = reader.fieldnames or []
        base_rows = list(reader)

    if target_col not in base_fieldnames:
        return 0, 0

    merged = 0
    skipped = 0

    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=base_fieldnames)
        writer.writeheader()

        for row in base_rows:
            key = normalize_text(row.get("KEY"))
            current_value = normalize_text(row.get(target_col))

            if key in translated_index and not current_value:
                row[target_col] = translated_index[key]
                merged += 1
            elif key in translated_index and current_value:
                skipped += 1

            writer.writerow(row)

    return merged, skipped


def main() -> None:
    parser = argparse.ArgumentParser(
        description="翻訳済み CSV (ai_translated_output) の内容を本番 CSV にマージする。"
    )
    parser.add_argument("base_dir", help="本番 CSV ディレクトリ（例: data/text）")
    parser.add_argument(
        "--translated-dir",
        default="ai_translated_output",
        help="翻訳済み CSV のサブディレクトリ（デフォルト: ai_translated_output）",
    )
    parser.add_argument(
        "--target-col",
        default="ja",
        help="マージ対象の列名（デフォルト: ja）",
    )
    parser.add_argument(
        "--inplace",
        action="store_true",
        help="本番ファイルを直接上書きする（指定しない場合は --output-dir に出力）",
    )
    parser.add_argument(
        "--output-dir",
        default="merged_output",
        help="出力サブディレクトリ（デフォルト: merged_output、--inplace 時は無視）",
    )
    args = parser.parse_args()

    base_dir = os.path.abspath(args.base_dir)
    if not os.path.isdir(base_dir):
        raise SystemExit(f"入力ディレクトリが存在しません: {base_dir}")

    translated_dir = os.path.join(base_dir, args.translated_dir)
    if not os.path.isdir(translated_dir):
        raise SystemExit(f"翻訳済みディレクトリが存在しません: {translated_dir}")

    if args.inplace:
        output_dir = base_dir
    else:
        output_dir = os.path.join(base_dir, args.output_dir)
        os.makedirs(output_dir, exist_ok=True)

    translated_files = sorted(
        name
        for name in os.listdir(translated_dir)
        if name.lower().endswith(".csv") and os.path.isfile(os.path.join(translated_dir, name))
    )

    total_merged = 0
    total_skipped = 0

    for name in translated_files:
        base_path = os.path.join(base_dir, name)
        translated_path = os.path.join(translated_dir, name)
        output_path = os.path.join(output_dir, name)

        if not os.path.isfile(base_path):
            print(f"{name}: skipped (not in base dir)")
            continue

        merged, skipped = merge_file(base_path, translated_path, output_path, args.target_col)
        total_merged += merged
        total_skipped += skipped
        print(f"{name}: merged {merged}, skipped {skipped} (already filled)")

    print("-" * 48)
    print(f"total merged: {total_merged}")
    print(f"total skipped (already filled): {total_skipped}")
    if not args.inplace:
        print(f"output dir: {output_dir}")


if __name__ == "__main__":
    main()
