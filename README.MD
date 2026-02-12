# 《Mewgenics》日本語翻訳パッチ
# Mewgenics Japanese Translation Patch

## 日本語説明

本プロジェクトは **《Mewgenics》の日本語翻訳パッチ** を提供しています。現在、ゲーム内の大部分のテキストをカバーしています：

- NPC の会話
- アイテム・スキルの説明
- UI テキスト
- 主要なゲーム内テキスト

本パッチは **AI 翻訳 + ゲーム内テスト校正** の方式で制作しており、今後もバージョンアップで翻訳品質と用語の統一を改善していきます。
フィードバックや修正提案を歓迎します。

### v0.2 更新内容
- `[img:...]` タグのパースエラーによるクラッシュ問題を修正
- スキル・アイテム説明文のレイアウトを最適化し、文字が小さくなる・詰まる問題を解消
- `en` 列の英語原文を復元し、言語切り替えが正常に動作しない問題を修正
- その他テキストの細かい修正とレイアウト最適化

### インストール方法
1. **Releases** ページから最新のパッチをダウンロード
2. パッチ内の `data` フォルダをゲームディレクトリ（`resources.gpak` と同じ階層）に展開
3. 同名ファイルを上書き
4. ゲームを起動し、設定から日本語を選択

### オプション：カスタムフォント

ゲームのデフォルトでは Noto Sans CJK TC（源ノ角ゴシック）がフォールバックフォントとして使用されます。別のフォントに変更したい場合、2つの方法があります：

**方法1：同梱フォントを使用（倉耳漁陽体）**
1. パッチ内の `swfs` フォルダをゲームディレクトリ（`resources.gpak` と同じ階層）にコピー
2. ゲームを再起動

> [倉耳漁陽体](https://tsanger.cn/product/199)は商用利用無料のフォントです。プレビューは下のスクリーンショットを参照してください。

**方法2：任意のフォントに差し替え（DIY）**
1. [JPEXS FFDec](https://github.com/jindrapetrik/jpexs-decompiler) をインストール
2. JPEXS FFDec のインストールディレクトリで以下のコマンドを実行（パッチ同梱の `swfs/unicodefont.swf` をベースに使用）：
   ```
   ffdec-cli.exe -replace unicodefont.swf unicodefont_custom.swf 1 your_font.ttf
   ```
3. 生成された `unicodefont_custom.swf` を `unicodefont.swf` にリネームし、ゲームディレクトリの `swfs/` フォルダに配置

> 注意：フォントファイルはゲームルートの `swfs/` フォルダに配置してください（`data/swfs/` ではありません）。

**デフォルトフォントに戻す場合：** ゲームディレクトリの `swfs/` フォルダを削除してください。

**プレビュー：**

| | デフォルトフォント（源ノ角ゴシック） | カスタムフォント（倉耳漁陽体） |
|:---:|:---:|:---:|
| メインメニュー | ![デフォルト-メニュー](screenshots/font_default_main_menu.png) | ![倉耳漁陽体-メニュー](screenshots/font_canger_yuyang_main_menu.png) |
| ゲーム設定 | ![デフォルト-設定](screenshots/font_default_settings_game.png) | ![倉耳漁陽体-設定](screenshots/font_canger_yuyang_settings_game.png) |

### 注意事項
- 現在は初期バージョンのため、一部未翻訳のテキストが残っている場合があります
- ゲームアップデートによりパッチが正常に動作しなくなった場合は、パッチの更新をお待ちください

### 謝辞
本プロジェクトは [rainzz-512/Mewgenics-Chinese-Translation](https://github.com/rainzz-512/Mewgenics-Chinese-Translation) をベースにしています。中国語翻訳パッチの制作者に感謝します。

---

## English

This project provides a **Japanese translation patch for Mewgenics**, covering most in-game texts including:

- NPC dialogues
- Items and abilities
- UI texts
- General gameplay text

The translation is created using **AI-assisted translation and in-game testing**, and future updates will continue to improve translation quality and terminology consistency.
Feedback and correction suggestions are welcome.

### v0.2 Changes
- Fixed parse errors caused by incorrect `[img:...]` tag parsing that could lead to crashes
- Improved line wrapping for ability and item descriptions to prevent cramped UI text
- Restored original English text in the `en` column, fixing language switching issues
- Minor text formatting and localization improvements

### Installation
1. Download the latest patch from **Releases**
2. Extract the `data` folder into the game directory (the same location as `resources.gpak`)
3. Overwrite files if prompted
4. Launch the game and select Japanese in the settings

### Optional: Custom Font

The game uses Noto Sans CJK TC as the fallback font option. You can replace it with a custom font:

**Option 1: Use the bundled font (CangEr YuYang)**
1. Copy the `swfs` folder from the patch into the game directory (same location as `resources.gpak`)
2. Restart the game

> CangEr YuYang is a free commercial-use font ([official page](https://tsanger.cn/product/199)). See screenshots below for preview.

**Option 2: DIY with any font**
1. Install [JPEXS FFDec](https://github.com/jindrapetrik/jpexs-decompiler)
2. Run the following command in the JPEXS FFDec installation directory (using the bundled `swfs/unicodefont.swf` as base):
   ```
   ffdec-cli.exe -replace unicodefont.swf unicodefont_custom.swf 1 your_font.ttf
   ```
3. Rename the output to `unicodefont.swf` and place it in the `swfs/` folder in the game directory

> Note: The font file goes in the `swfs/` folder at game root (NOT `data/swfs/`).

**Restore default font:** Simply delete the `swfs/` folder in the game directory.

**Preview:**

| | Default Font (Noto Sans CJK) | Custom Font (CangEr YuYang) |
|:---:|:---:|:---:|
| Main Menu | ![Default-Menu](screenshots/font_default_main_menu.png) | ![CangEr-Menu](screenshots/font_canger_yuyang_main_menu.png) |
| Settings | ![Default-Settings](screenshots/font_default_settings_game.png) | ![CangEr-Settings](screenshots/font_canger_yuyang_settings_game.png) |

### Notes
- This is an early release; some lines may remain untranslated
- If a game update breaks the patch, please wait for a new version

### Credits
This project is based on [rainzz-512/Mewgenics-Chinese-Translation](https://github.com/rainzz-512/Mewgenics-Chinese-Translation). Thanks to the original Chinese translation patch authors.

