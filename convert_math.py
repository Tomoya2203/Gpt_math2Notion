import os
import requests
import json
import re
import time
import logging
import copy
from functools import wraps

# --- ユーザー設定 --------------------------------------------------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
PAGE_ID = os.getenv("PAGE_ID")
# -----------------------------------------------------------------------------------

# --- 基本設定 ----------------------------------------------------------------------
# [改善] ロガーの設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

NOTION_API_URL = "https://api.notion.com/v1"
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}
NOTION_EXPRESSION_LIMIT = 1000

# [改善] 箇条書きや見出しなど、インライン数式を含む可能性があるブロックタイプを拡充
RICH_TEXT_HOLDER_TYPES = [
    "paragraph",
    "bulleted_list_item",
    "numbered_list_item",
    "to_do",
    "toggle",
    "quote",
    "callout",
    "heading_1",
    "heading_2",
    "heading_3"
]

# [改善] 処理中に失敗したブロックを記録するリスト
failed_blocks = []


# --- APIリクエストのラッパー (レート制限対応) -------------------------------------------
# [改善] APIリクエストにレート制限(429)リトライ機能を追加するデコレータ
def handle_rate_limit(max_retries=5):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    if e.response is not None and e.response.status_code == 429:
                        retry_after = int(e.response.headers.get("Retry-After", 1))
                        logging.warning(
                            f"レート制限に達しました。{retry_after}秒待機してリトライします... ({attempt + 1}/{max_retries})")
                        time.sleep(retry_after)
                    else:
                        logging.error(f"リクエスト中にエラーが発生しました: {e}")
                        # [改善] 失敗したブロックIDを記録
                        if 'url' in kwargs and 'blocks/' in kwargs['url']:
                            block_id = kwargs['url'].split('blocks/')[1].split('/')[0]
                            failed_blocks.append(block_id)
                        raise  # 429以外のエラーは再スロー
            logging.error(f"{max_retries}回リトライしましたが、APIリクエストに失敗しました。")
            raise Exception("API request failed after multiple retries")

        return wrapper

    return decorator


@handle_rate_limit()
def make_request(method, url, **kwargs):
    """汎用的なリクエスト送信関数"""
    response = requests.request(method, url, headers=HEADERS, **kwargs)
    response.raise_for_status()
    return response


# --- Notion API 操作関数 ------------------------------------------------------------
def get_all_child_blocks(block_id):
    """指定されたブロックの子ブロックを、ページネーションを考慮してすべて取得する"""
    all_blocks = []
    url = f"{NOTION_API_URL}/blocks/{block_id}/children"
    params = {"page_size": 100}
    try:
        while True:
            response = make_request("GET", url, params=params)
            data = response.json()
            all_blocks.extend(data["results"])
            if data.get("has_more"):
                params["start_cursor"] = data["next_cursor"]
            else:
                break
    except Exception as e:
        logging.error(f"ブロック(ID: {block_id})の子ブロック取得に失敗しました。詳細: {e}")
        failed_blocks.append(block_id)
        return None
    return all_blocks


def archive_block(block_id):
    """指定されたブロックをアーカイブする"""
    try:
        url = f"{NOTION_API_URL}/blocks/{block_id}"
        make_request("PATCH", url, json={"archived": True})
        logging.info(f"  - 元のブロック {block_id} をアーカイブしました。")
    except Exception as e:
        logging.error(f"ブロック {block_id} のアーカイブに失敗しました。詳細: {e}")


def insert_equation_block_after(parent_id, expression, after_block_id):
    """指定されたブロックの後に新しい数式ブロックを挿入する"""
    try:
        url = f"{NOTION_API_URL}/blocks/{parent_id}/children"
        payload = {
            "after": after_block_id,
            "children": [{"type": "equation", "equation": {"expression": expression}}]
        }
        make_request("PATCH", url, json=payload)
        logging.info(f"  - 数式ブロックを挿入しました: {expression[:40]}...")
        return True
    except Exception as e:
        logging.error(f"数式ブロックの挿入に失敗しました。詳細: {e}")
        return False


def update_block(block_id, payload):
    """汎用的なブロック更新関数"""
    try:
        url = f"{NOTION_API_URL}/blocks/{block_id}"
        make_request("PATCH", url, json=payload)
        logging.info(f"  - ブロック {block_id} ({list(payload.keys())[0]}) を更新しました。")
    except Exception as e:
        logging.error(f"ブロック {block_id} の更新に失敗しました。詳細: {e}")

def parse_inline_equations(full_text, block_id):
    """テキストを解析し、インライン数式を含むリッチテキストに変換する"""
    # パターンは変更なし: $で始まり$で終わるペアを探す。ただし、$$は除外。
    # 中身はエスケープされた文字(\\.)か、$以外の文字([^$])の繰り返し。
    pattern = r"\$((?:\\.|[^$])+?)\$"

    new_rich_text = []
    last_end = 0
    is_changed = False

    for match in re.finditer(pattern, full_text):
        start, end = match.span()
        # 数式前のテキスト部分
        if start > last_end:
            new_rich_text.append({"type": "text", "text": {"content": full_text[last_end:start]}})

        # [修正箇所]
        # unicode_escapeによるLaTeXコマンドの破壊を防ぐため、明示的な置換に切り替える。
        raw_expression = match.group(1)
        # Notionの数式内で意味を持つエスケープシーケンス(\\ と \$)のみを元に戻す。
        expression = raw_expression.replace('\\\\', '\\').replace('\\$', '$')

        if len(expression) > NOTION_EXPRESSION_LIMIT:
            logging.warning(f"インライン数式が長すぎるためテキストとして扱います。 Block ID: {block_id}")
            new_rich_text.append({"type": "text", "text": {"content": match.group(0)}})  # $...$ をそのままテキストに
        elif expression:
            new_rich_text.append({"type": "equation", "equation": {"expression": expression}})
            is_changed = True

        last_end = end

    # 最後の数式以降のテキスト部分
    if last_end < len(full_text):
        new_rich_text.append({"type": "text", "text": {"content": full_text[last_end:]}})

    # リッチテキストが空になるのを防ぐ
    if not new_rich_text and full_text:
        return False, [{"type": "text", "text": {"content": full_text}}]

    return is_changed, new_rich_text


# --- メイン処理 --------------------------------------------------------------------
def process_blocks_recursively(block_id):
    """ブロックを再帰的に処理し、LaTeXを変換する"""
    blocks = get_all_child_blocks(block_id)
    if blocks is None: return

    for block in blocks:
        block_type = block["type"]

        if block["has_children"]:
            logging.info(f"\n子ブロックを探索中: {block['id']} ({block_type})")
            process_blocks_recursively(block["id"])

        # 一般的なリッチテキストを持つブロックの処理
        if block_type in RICH_TEXT_HOLDER_TYPES:
            rich_text_list = block.get(block_type, {}).get("rich_text", [])
            if not rich_text_list: continue

            full_text = "".join([rt.get("plain_text", "") for rt in rich_text_list])

            # [改善] ブロック数式の正規表現をより厳密に
            block_match = re.fullmatch(r"\s*\$\$(.*?)\$\$\s*", full_text, re.DOTALL)
            if block_match:
                expression = block_match.group(1).strip()

                if len(expression) > NOTION_EXPRESSION_LIMIT:
                    logging.warning(
                        f"ブロック {block['id']} の数式が長すぎるためスキップします。（{len(expression)} > {NOTION_EXPRESSION_LIMIT} chars）")
                    continue

                logging.info(f"\n[ブロック数式] を検出: {block['id']}")
                parent_info = block["parent"]
                parent_id = parent_info[parent_info["type"]]

                if insert_equation_block_after(parent_id, expression, block["id"]):
                    archive_block(block["id"])
                continue

            # インライン数式の処理
            is_changed, new_rich_text = parse_inline_equations(full_text, block["id"])
            if is_changed:
                logging.info(f"\n[インライン数式] を検出: {block['id']}")
                # [改善] to_doの'checked'など、他のプロパティを壊さないように更新
                payload_content = copy.deepcopy(block[block_type])
                payload_content["rich_text"] = new_rich_text
                update_block(block["id"], {block_type: payload_content})

        # テーブル行の処理
        elif block_type == "table_row":
            cells = block.get("table_row", {}).get("cells", [])
            if not cells: continue

            new_cells = []
            is_row_changed = False
            for cell_rich_text_list in cells:
                full_text = "".join([rt.get("plain_text", "") for rt in cell_rich_text_list])
                is_cell_changed, new_cell_rich_text = parse_inline_equations(full_text, block["id"])

                if is_cell_changed:
                    new_cells.append(new_cell_rich_text)
                    is_row_changed = True
                else:
                    # [改善] 変更がないセルもdeepcopyで安全にコピー
                    new_cells.append(copy.deepcopy(cell_rich_text_list))

            if is_row_changed:
                logging.info(f"\n[テーブル内インライン数式] を検出: {block['id']}")
                update_block(block["id"], {"table_row": {"cells": new_cells}})


def main():
    """メイン処理"""
    if not NOTION_TOKEN or not PAGE_ID:
        logging.error("環境変数 `NOTION_TOKEN` と `PAGE_ID` を設定するか、スクリプトを直接編集してください。")
        return

    logging.warning("このスクリプトはNotionページの内容を直接変更します。")
    logging.warning("必ず事前にページのバックアップ（複製）を作成してください。")
    if input("処理を続行しますか？ (y/n): ").lower() != 'y':
        logging.info("処理を中断しました。")
        return

    logging.info(f"\n処理開始: ページID {PAGE_ID} ...")
    try:
        process_blocks_recursively(PAGE_ID)
    except Exception as e:
        logging.critical(f"処理中に予期せぬエラーが発生し、中断しました: {e}")
    finally:
        if failed_blocks:
            logging.warning("\n--- 処理に失敗したブロックID一覧 ---")
            for block_id in set(failed_blocks):  # 重複を除外して表示
                logging.warning(f" - {block_id}")
            logging.warning("------------------------------------")

        logging.info("\n処理が完了しました。Notionページをリロードして結果を確認してください。")


if __name__ == "__main__":
    main()