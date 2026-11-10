# Lacuna Live → Google Docs bridge

獨立 Node.js 24 CLI，把指定 gotranslive live session 的 committed 段落追加到 Google 文件，並在連線期間把 panel 的段落修改同步為取代、刪除同步為刪除線；不需要修改 gotranslive。

## 安裝與 Google 授權

1. 安裝 Node.js 24 或更新版本，執行 `npm ci`。
2. 在 Google Cloud 建立專案，啟用 **Google Docs API**。
3. 建立 Service Account，取得 JSON key，放在此 repository 外的安全位置；限制檔案權限，不可提交到 Git。
4. 將既有 Google 文件分享給 Service Account JSON 的 `client_email`，授予「編輯者」。不需要讓文件公開。Workspace 禁止外部分享時，需先由管理員允許這個身分。
5. 複製 `.env.example` 為 `.env`，填入主機 URL、session ID、document ID 和憑證的絕對路徑。
6. 執行 `npm start`，以 Ctrl+C 或 SIGTERM 停止。

```sh
cp .env.example .env
# 編輯 .env 後
npm start
```

文件網址 `/document/d/DOCUMENT_ID/edit` 中的 `DOCUMENT_ID` 即設定值。多分頁文件建議填入目標 `GOOGLE_TAB_ID`（網址的 `tab` 參數）。未填時使用文件第一個分頁；請勿在執行期間重新排序分頁。

`OUTPUT=corrected` 使用校正原文；`OUTPUT=text` 使用原始辨識；其他值直接作為 `result.translated` 的語言 key，例如 `en`、`ja`。每段追加一個換行，不加入時間戳、不轉換顯示字典。選定文字缺失或空白時會停止程序而非偷偷略過，游標不會越過該筆；請檢查 session 的輸出設定。

## 運作與恢復

- 訂閱 `/api/session/{sid}/stream`，不要求 snapshot、不匯入啟動前歷史。viewer SSE 不需要 API key 或 panel secret。
- 只接受有 SSE id、`partial=false` 且非 flow-only 的 `transcription_update` 作為新段落；重播不同版本不會覆蓋已保存文字。忽略 viewer clear 與其他事件。
- SQLite 以段落 `start_time` 去重（gotranslive 的 SSE id 即由它產生；兩者不一致時記錄警告並以 `start_time` 為準，不中止），原始文字與接收游標在同一個 transaction 保存。
- `segment_updated`：選定輸出的文字有變才處理。尚未寫入的段落直接改佇列；已寫入（或寫入中）的段落排入待更新，之後刪除該段的 named range 內容、插入新文字並重建同名 range。同段多次修改只寫最後一版。
- `segment_deleted`：尚未寫入的段落直接不寫；已寫入的段落加上刪除線，之後的修改不再處理。
- 修改與刪除同樣使用 `requiredRevisionId`、寫入 SQLite 後才算待處理；取代與刪除線都可重複套用，結果不明時直接重試。
- 每 3 秒嘗試寫入，最多合併 100 段／約 20,000 UTF-16 單位；單段超過此長度不截斷。可用 `FLUSH_INTERVAL_MS` 調整（最少 1 秒）。
- 使用 Docs `requiredRevisionId` 防止讀取後的共編變更造成位置錯誤。衝突或 API 失敗會保留佇列並退避重試，最長約 61 秒。
- 每批文字與每段一個 named range 在同一個 Google 原子請求建立（range 不含段尾換行）。網路結果不明或程序中斷後，讀回 range 確認是否成功，避免盲目重複追加。range 不是可見文字，會持續保留。
- 停止時不強制清空待寫內容；重新使用同一個 state 啟動會續寫。不要刪除 SQLite、WAL 或 SHM 檔；備份請先停機。
- 同一 state 只能啟動一個程序，以 `.lock` 檔阻擋重複實例。SIGKILL／斷電後鎖檔可能殘留：**先確認原程序確實已停止**，再手動移除對應 `.lock`。不要用不同 state 同時同步同一 session 到同一文件。
- state 綁定來源、session、文件、tab 設定及輸出語言；設定不符會拒絕啟動。新的獨立工作請使用不同 `STATE_PATH`，但新 state 不會識別舊 state 寫過的段落。

## 已知限制

1. 「首次收到」不等於「最初提交」：斷線後 Redis replay 提供的段落可能已經修訂；gotranslive 沒有永久原始 commit 事件日誌，本工具不能還原已失去的版本。
2. 首次連線之前，以及還沒收到任何 commit 就斷線的空窗，不保證補回；收到首筆後才有可持久化的 SSE 游標。長時間斷線、Redis cache 遺失或重置也可能漏段，不會自動用資料庫當前版本冒充原始版本補齊。
3. 不跟隨 session 結束／封存／重用來切換文件。對 live SID 持續監聽；若同 SID 再開始新場次，也會繼續追加到同份文件。請由操作者停止及更換設定。
4. 請勿刪除或破壞 bridge 建立的 named ranges；若標記遺失，結果不明批次的防重能力也會失效。手動刪除已確認文字不會讓 bridge 自動補回。
5. 多人共編可以造成 revision 衝突及延遲。請勿在 bridge 管理的文字區域大量重排／刪除；追加不是文件鏡像或雙向同步。
6. Google 預設每使用者每專案每分鐘 60 次寫入、每專案 600 次。多份文件共用 Service Account 的配額。以 Cloud Console 實際配額為準。
7. SQLite 保留已處理文字以維持去重，檔案及 Google named ranges 隨內容增加；適合臨時／單場用途，不做自動輪替。Google 文件本身也有大小限制。
8. Google 憑證錯誤、撤權、網路故障會阻止寫入；執行期間會保留佇列並持續記錄失敗。沒有另設監控或容量上限，需留意磁碟與 stderr。
9. 修改同步只涵蓋連線期間收到的 panel 事件：`segment_updated`／`segment_deleted` 沒有 SSE id，斷線期間的修改不會重播；owner 逐字稿編輯器的修改與刪除 gotranslive 不發事件；API key 也沒有可讀段落的端點可事後比對。
10. 修改會整段覆蓋 bridge 管理的那一行，文件中對該行的手動修改會被蓋掉。若有人把該行整段刪除（range 消失），之後的修改記錄警告後放棄，不會補回。
11. 本版之前寫入的段落只有批次標記、沒有逐段 range，它們的修改與刪除會記錄警告並略過。

## 驗證

`npm test` 啟動本機 HTTP SSE 與 Docs API 模擬端點（模擬文件會隨插入／刪除移動 named range），透過真實 fetch、官方 Google client 與磁碟 SQLite 驗證：忽略 partial／flow、首次版本不覆蓋、Unicode 範圍、重啟續寫、Google 已成功但 HTTP 回應遺失時不重複追加，以及修改取代、刪除線、寫入中修改、重複修改、文件端已刪除時放棄。

這不代表已通過真實 Google 授權或正式 session 驗證。部署後請在專用測試文件啟動 bridge，提交一句內容，確認文件追加、重新啟動後不重複；再從 panel 修改與刪除一句，確認取代與刪除線，然後切換正式文件。

## API 參考

- [Google Docs batchUpdate 與 revision control](https://developers.google.com/workspace/docs/api/reference/rest/v1/documents/batchUpdate)
- [Google Docs named ranges（取代內容的官方作法）](https://developers.google.com/workspace/docs/api/how-tos/named-ranges)
- [Google Docs API 配額](https://developers.google.com/workspace/docs/api/limits)
- gotranslive：`../gotranslive/docs/http-api.md`
