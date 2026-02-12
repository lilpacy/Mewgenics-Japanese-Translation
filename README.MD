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

### v0.6 更新内容
- 根据官方 Patch Notes 1.0.20738，本地化 CSV 已由原先约 20 个拆分文件合并为单一文件 `data/text/combined.csv`
- 本仓库已同步调整结构：`data/text` 目录仅保留 `combined.csv` 作为当前维护入口
- 旧拆分 CSV 已迁移到仓库根目录下的 `_archived_csv`，用于回溯与比对，不再作为当前版本主编辑目标
- 修复了个别文本翻译与措辞问题，提升局部语句准确性与可读性
- 新增了一批中文猫名（含公猫、母猫与中性）

### v0.5 更新内容
- 针对《以撒的结合》等联动物品，手工调整对应术语与原游戏一致（如 `Guppy` 统一译为“嗝屁猫”；以撒联动道具优先沿用以撒社区常见译法，如神性套装三个部件译为“肉体”“思想”“灵魂”）  
- 对 `events` 与 `npc_dialog` 进行了大幅文本更新与本地化优化，显著提升语句自然度与上下文连贯性  
- 针对跨文件术语一致性问题进行了集中优化，提升翻译质量与上下文连贯性  
- 结合模型推理与人工校对，修复并优化了多处关键词在不同语境下的译法  
- 新增包含中文猫名的文档：`catnames_male_en.txt`、`catnames_female_en.txt`、`catnames_neutral_en.txt`  
- 如需在游戏中启用中文猫名，请将以上 3 个 `txt` 文件放入 `data` 文件夹（与 `text` 文件夹同级）

### v0.4 更新内容
- 对 `events` 与 `npc_dialog` 进行了整体验证与本地化润色，提升语句自然度与上下文一致性  
- 统一游戏内关键词翻译，并修复一小部分错翻与漏翻  
- 更新能力与被动描述中的术语，统一为“装备槽”以提高翻译一致性

### v0.3 更新内容
- 修复并优化残留的部分文本挤压、缩小显示问题  
- 补充部分未翻译与翻译不完整的文本  
- 统一部分关键词翻译（如 `Thorns` 统一为“荆棘”、`Bruise` 统一为“挫伤”）

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
2. このリポジトリから `Mewgenics_CN_patch/swfs/unicodefont.swf` をダウンロードし、カスタムフォントファイルと一緒に JPEXS FFDec のインストールディレクトリに配置
3. カスタムフォントファイル名から**スペース**をすべて削除。例：`My Font.ttf` → `MyFont.ttf`
4. コマンドプロンプトを開き、JPEXS FFDec のインストールディレクトリに移動
5. 以下のコマンドを実行（`MyFont.ttf` を実際のフォントファイル名に置き換え）：
   ```
   ffdec-cli.exe -replace unicodefont.swf unicodefont_custom.swf 1 MyFont.ttf
   ```
6. JPEXS FFDec のインストールディレクトリに `unicodefont_custom.swf` が生成されます。**ゲームルートディレクトリ**に `swfs\` フォルダを作成し、このファイルを配置して `unicodefont.swf` にリネーム

> 注意：最終的な `.swf` ファイルは**ゲームルートディレクトリ**の `swfs\` フォルダに配置してください（JPEXS FFDec のインストールディレクトリや `data\` 配下の `swfs` フォルダではありません）。

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

### v0.6 Changes
- Synced with official Patch Notes 1.0.20738: localization CSVs were merged from ~20 split files into a single `data/text/combined.csv`
- Repository structure has been updated accordingly: `data/text` now keeps only `combined.csv` as the active maintenance target
- Legacy split CSV files were moved to `_archived_csv` at repository root for reference/diffing, and are no longer the primary editing target for current versions
- Fixed a small number of translation issues and wording inconsistencies to improve local text accuracy and readability
- Added more Chinese cat names (male/female/neutral)

### v0.5 Changes
- Manually aligned crossover item terminology (especially for *The Binding of Isaac* references) with established in-game naming conventions (e.g., `Guppy` -> “嗝屁猫”; Isaac crossover terms follow Isaac-localization conventions, including “肉体 / 思想 / 灵魂” for the Godhead set components)  
- Performed major text updates and localization optimization for `events` and `npc_dialog`, significantly improving fluency and contextual coherence  
- Improved cross-file terminology consistency to enhance translation quality and context coherence  
- Combined model-assisted refinement with manual review to fix and polish term usage across different contexts  
- Added documents containing Chinese cat names: `catnames_male_en.txt`, `catnames_female_en.txt`, `catnames_neutral_en.txt`  
- To use Chinese cat names in-game, place the 3 `txt` files into the `data` folder (same level as the `text` folder)

### v0.4 Changes
- Performed full localization polish for `events` and `npc_dialog`, improving fluency and contextual consistency  
- Unified in-game keyword translations and fixed a small subset of mistranslations/missing lines  
- Updated terminology in ability/passive descriptions, standardizing to "装备槽" for consistency

### v0.3 Changes
- Fixed and optimized remaining cases of compressed/shrunk text display  
- Added some previously untranslated or partially translated text  
- Standardized terminology for key terms (e.g., `Thorns` as "荆棘", `Bruise` as "挫伤")

### v0.2 Changes
- Fixed parse errors caused by incorrect `[img:...]` tag parsing that could lead to crashes
- Improved line wrapping for ability and item descriptions to prevent cramped UI text
- Restored original English text in the `en` column, fixing language switching issues
- Minor text formatting and localization improvements

### Chinese Cat Name RFC
- We welcome community suggestions for Chinese cat names and will evaluate them for inclusion in future patch updates  
- Any naming style is welcome (serious, playful, classical, modern, etc.)  
- Added blank template: `Mewgenics_CN_patch/catnames_community_template.csv`  
- How to fill: column 1 = male names, column 2 = female names, column 3 = neutral names; one name per cell, empty cells are allowed, and please keep the header and column order unchanged  
- Current Chinese cat-name docs: `catnames_male_en.txt`, `catnames_female_en.txt`, `catnames_neutral_en.txt`  
- To make them work in-game, place the 3 `txt` files into the `data` folder (same level as the `text` folder)  
- To avoid moderation and legal issues, the following categories are excluded:  
   - Real-person names (public figures, creators, streamers, etc.)  
   - Illegal, hateful, discriminatory, political-sensitive, explicit/sexual content  
   - Clearly infringing trademark/brand names or copyrighted character names  
   - Provocative, abusive, or harassment-oriented naming

### Acknowledgements
- Thanks to [infinityedge01](https://github.com/infinityedge01), [elmagnificogi](https://github.com/elmagnificogi), [Supuritto](https://github.com/Supuritto), [TeddyHuang-00](https://github.com/TeddyHuang-00), [Xsakura1314](https://github.com/Xsakura1314), [Claude-yuliu](https://github.com/Claude-yuliu), [tuwary](https://github.com/tuwary), [MnFeN](https://github.com/MnFeN), and [mewcodex](https://github.com/mewcodex) for contributions and support during patch development  
- Thanks to [DR9K69AI79](https://github.com/DR9K69AI79) for font modification support

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
2. Download `Mewgenics_CN_patch/swfs/unicodefont.swf` from this repository and place it along with your custom font file in the JPEXS FFDec installation directory
3. Remove all **spaces** from the custom font filename. For example, if the file is named `My Font.ttf`, it should be renamed to `MyFont.ttf`
4. Open the Command Prompt. [Set the current directory to the JPEXS FFDec installation directory](https://www.geeksforgeeks.org/techtips/change-directories-in-command-prompt/)
5. In the Command Prompt, run the following command (replace `MyFont.ttf` in the command with the renamed font filename):
   ```
   ffdec-cli.exe -replace unicodefont.swf unicodefont_custom.swf 1 MyFont.ttf
   ```
6. A file named `unicodefont_custom.swf` will be generated in the JPEXS FFDec installation directory. Create a `swfs\` folder in the **game root directory**, place this file in that folder, and then rename it to `unicodefont.swf`,

> Note: The final `.swf` file goes in the `swfs/` folder at **game root** (NOT in the JPEXS FFDec installation directory, nor under `data\`).

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

