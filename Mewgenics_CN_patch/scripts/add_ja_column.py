import argparse
import csv
import os
from typing import List


def add_column_after(fieldnames: List[str], after_col: str, new_col: str) -> List[str]:
    if new_col in fieldnames:
        return fieldnames

    if after_col in fieldnames:
        idx = fieldnames.index(after_col) + 1
        return fieldnames[:idx] + [new_col] + fieldnames[idx:]

    return fieldnames + [new_col]


def process_file(file_path: str, output_path: str, new_col: str, after_col: str) -> bool:
    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        original_fieldnames = reader.fieldnames or []
        rows = list(reader)

    if new_col in original_fieldnames:
        return False

    new_fieldnames = add_column_after(list(original_fieldnames), after_col, new_col)

    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=new_fieldnames)
        writer.writeheader()
        for row in rows:
            if new_col not in row:
                row[new_col] = ""
            writer.writerow(row)

    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CSV ファイルに新しい言語列を追加する（デフォルト: zh の後に ja 列を追加）。"
    )
    parser.add_argument("input_dir", help="CSV が格納されたディレクトリ")
    parser.add_argument(
        "--column",
        default="ja",
        help="追加する列名（デフォルト: ja）",
    )
    parser.add_argument(
        "--after",
        default="zh",
        help="この列の後に挿入する（デフォルト: zh）",
    )
    parser.add_argument(
        "--inplace",
        action="store_true",
        help="元ファイルを直接上書きする（指定しない場合は --output-dir に出力）",
    )
    parser.add_argument(
        "--output-dir",
        default="with_ja_column",
        help="出力サブディレクトリ（デフォルト: with_ja_column、--inplace 時は無視）",
    )
    args = parser.parse_args()

    input_dir = os.path.abspath(args.input_dir)
    if not os.path.isdir(input_dir):
        raise SystemExit(f"入力ディレクトリが存在しません: {input_dir}")

    if args.inplace:
        output_dir = input_dir
    else:
        output_dir = os.path.join(input_dir, args.output_dir)
        os.makedirs(output_dir, exist_ok=True)

    csv_files = sorted(
        name
        for name in os.listdir(input_dir)
        if name.lower().endswith(".csv") and os.path.isfile(os.path.join(input_dir, name))
    )

    added = 0
    skipped = 0

    for name in csv_files:
        src = os.path.join(input_dir, name)
        dst = os.path.join(output_dir, name)

        changed = process_file(src, dst, args.column, args.after)
        if changed:
            added += 1
            print(f"{name}: added '{args.column}' column")
        else:
            skipped += 1
            print(f"{name}: skipped ('{args.column}' already exists)")

    print("-" * 48)
    print(f"files with column added: {added}")
    print(f"files skipped: {skipped}")
    if not args.inplace:
        print(f"output dir: {output_dir}")


if __name__ == "__main__":
    main()
