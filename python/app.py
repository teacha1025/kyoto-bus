import streamlit as st
import pandas as pd
import streamlit.components.v1 as components

st.set_page_config(page_title="京都市バス 時刻表", layout="wide")

st.markdown("""
<div class="no-print">
    <h1 style="font-size: 2.25rem; font-weight: 600; padding-bottom: 1rem;">京都市営バス 時刻表アプリ</h1>
    <p>京都市が公開している<b>市バスのGTFSオープンデータ</b>を読み込み、停留所・方面別に時刻表を生成するアプリです。<br>
    出発停留所と到着停留所を指定することで、その区間を通る全系統の時刻表をまとめて表示します。</p>
</div>
""", unsafe_allow_html=True)

# データの読み込み (キャッシュ機能を使って2回目以降の表示を高速化)
@st.cache_data
def load_data():
    try:
        # IDなどの列が意図せず数値変換されないよう、全て文字列(str)として読み込む
        stops = pd.read_csv("city_bus_data/stops.txt", dtype=str, encoding='utf-8')
        routes = pd.read_csv("city_bus_data/routes.txt", dtype=str, encoding='utf-8')
        trips = pd.read_csv("city_bus_data/trips.txt", dtype=str, encoding='utf-8')
        stop_times = pd.read_csv("city_bus_data/stop_times.txt", dtype=str, encoding='utf-8')
        calendar = pd.read_csv("city_bus_data/calendar.txt", dtype=str, encoding='utf-8')
        return stops, routes, trips, stop_times, calendar
    except FileNotFoundError:
        return None, None, None, None, None

stops, routes, trips, stop_times, calendar = load_data()

if stops is None:
    st.error("⚠️ GTFSデータが見つかりません。アプリと同じディレクトリに `stops.txt`, `routes.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt` を配置してください。")
else:
    # 1. 出発・到着停留所の選択UI
    # 欠損値を除外し、あいうえお順にソートしてドロップダウンメニュー化
    stop_names = sorted(stops['stop_name'].dropna().unique())
    
    col1, col2 = st.columns(2)
    with col1:
        selected_dep_stop = st.selectbox("🚏 出発停留所を選択", stop_names, index=stop_names.index("桂駅東口") if "桂駅東口" in stop_names else 0)
    with col2:
        selected_arr_stop = st.selectbox("🚏 到着停留所を選択", stop_names, index=stop_names.index("ＪＲ桂川駅前") if "ＪＲ桂川駅前" in stop_names else 0)

    if selected_dep_stop and selected_arr_stop:
        if selected_dep_stop == selected_arr_stop:
            st.warning("出発停留所と到着停留所には異なる停留所を選択してください。")
        else:
            # 選択された停留所の stop_id を取得
            dep_stop_ids = stops[stops['stop_name'] == selected_dep_stop]['stop_id'].tolist()
            arr_stop_ids = stops[stops['stop_name'] == selected_arr_stop]['stop_id'].tolist()

            # 2. 各停留所を通る便のデータを抽出
            dep_st = stop_times[stop_times['stop_id'].isin(dep_stop_ids)][['trip_id', 'departure_time', 'stop_sequence']]
            arr_st = stop_times[stop_times['stop_id'].isin(arr_stop_ids)][['trip_id', 'stop_sequence']]

            # 3. 同じ便（trip_id）で両方の停留所を通るものを結合
            merged_st = pd.merge(dep_st, arr_st, on='trip_id', suffixes=('_dep', '_arr'))

            # 通過順（stop_sequence）を比較し、出発停留所が先にある便のみに絞り込む
            merged_st['stop_sequence_dep'] = pd.to_numeric(merged_st['stop_sequence_dep'])
            merged_st['stop_sequence_arr'] = pd.to_numeric(merged_st['stop_sequence_arr'])
            valid_trips = merged_st[merged_st['stop_sequence_dep'] < merged_st['stop_sequence_arr']]
            
            # ループ系統などで同じ便が複数回マッチする（重複する）のを防ぐため、trip_idで一意にする
            valid_trips = valid_trips.sort_values('stop_sequence_dep').drop_duplicates(subset=['trip_id'])

            if valid_trips.empty:
                st.warning(f"「{selected_dep_stop}」から「{selected_arr_stop}」へ向かう直通バスは見つかりませんでした。")
            else:
                # 4. 系統データ(routes)と行き先データ(trips)を結合して詳細情報を付与
                valid_st = pd.merge(valid_trips, trips, on="trip_id")
                valid_st = pd.merge(valid_st, routes, on="route_id")

                # 必要なカラムに絞り、出発時刻順にソート
                timetable = valid_st[['departure_time', 'route_short_name', 'trip_headsign', 'service_id']].copy()
                timetable = timetable.dropna(subset=['departure_time'])
                timetable = timetable.sort_values('departure_time')
                # 「市バス」の表記をあらかじめ削除しておく
                timetable['route_short_name'] = timetable['route_short_name'].str.replace('市バス', '', regex=False)

                st.subheader(f"{selected_dep_stop} ➔ {selected_arr_stop} ")
                
                # 凡例を作成（系統ごとに ①, ②, ③... などの記号を割り当て）
                unique_routes = sorted(timetable['route_short_name'].unique())
                route_symbols = {route: chr(9312 + i) if i < 20 else f"[{i+1}]" for i, route in enumerate(unique_routes)}
                legend_text = "**凡例:**　" + " ｜ ".join([f"{symbol}: {route}系統" for route, symbol in route_symbols.items()])
                st.markdown(legend_text)

                # calendar.txtからダイヤIDと「平日」「土曜」「休日」の対応付けを作成
                service_mapping = {}
                for _, row in calendar.iterrows():
                    if row.get('monday') == '1':
                        service_mapping[row['service_id']] = '平日'
                    elif row.get('saturday') == '1':
                        service_mapping[row['service_id']] = '土曜'
                    elif row.get('sunday') == '1':
                        service_mapping[row['service_id']] = '休日'

                service_ids = sorted(timetable['service_id'].unique())
                schedule_data = {}
                
                for sid in service_ids:
                    df_service = timetable[timetable['service_id'] == sid].copy()
                    
                    # 時刻を「時」と「分」に分割
                    df_service['hour'] = df_service['departure_time'].str.split(':').str[0].astype(int)
                    
                    # 分の数字を大きく、注釈記号を一回り小さくするためのHTMLタグを適用
                    minute_str = df_service['departure_time'].str.split(':').str[1]
                    symbol_str = df_service['route_short_name'].map(route_symbols)
                    df_service['minute_display'] = "<span style='font-size: 1.25em; font-weight: 500;'>" + minute_str + "</span><span style='font-size: 0.85em;'>" + symbol_str + "</span>"
                    
                    # ダイヤ名を取得
                    day_name = service_mapping.get(sid, f"ダイヤID: {sid}")
                    
                    # 時間(hour)ごとにグループ化してテキストを結合
                    schedule_data[day_name] = df_service.groupby('hour')['minute_display'].apply(lambda x: "　".join(x))
                
                # すべてのダイヤを1つのデータフレームに結合（無い時間帯のNaNは空文字で埋める）
                combined_df = pd.DataFrame(schedule_data).fillna("")
                
                # 「平日」「土曜」「休日」の順になるように列を並び替え
                ordered_cols = []
                for day in ['平日', '土曜', '休日']:
                    if day in combined_df.columns:
                        ordered_cols.append(day)
                for col in combined_df.columns:
                    if col not in ordered_cols:
                        ordered_cols.append(col)
                combined_df = combined_df[ordered_cols]

                # インデックス（時）の表示を整形し、HTMLタグでサイズを調整
                combined_df.index = combined_df.index.map(lambda x: f"<span style='font-size: 1.25em; font-weight: 500;'>{x % 24:02d}</span>" if x < 24 else f"<span style='font-size: 1.25em; font-weight: 500;'>翌{x % 24:02d}</span>")
                
                # インデックスを列に変換し、列名を「時」にする
                combined_df = combined_df.reset_index()
                combined_df.rename(columns={'hour': '時'}, inplace=True)
                
                # テーブルを枠いっぱいまで広げ、各ダイヤの幅を均等にするCSS
                st.markdown("""
                <style>
                table.dataframe {
                    width: 100% !important;
                    word-break: keep-all !important;
                    table-layout: fixed !important;
                    border-collapse: collapse;
                }
                table.dataframe th, table.dataframe td {
                    border: 1px solid var(--border-color, #e0e0e0);
                    padding: 0.5rem;
                }
                table.dataframe th {
                    background-color: var(--secondary-background-color, #f0f2f6);
                    text-align: center !important;
                }
                /* 1列目（時間の列）のみ幅を固定し、残りを均等に分ける */
                table.dataframe th:first-child,
                table.dataframe td:first-child {
                    width: 5rem !important;
                    text-align: center;
                }
                /* 1行おきに背景色を付ける（ストライプ） */
                table.dataframe tbody tr:nth-child(even) {
                    background-color: rgba(128, 128, 128, 0.05);
                }
                /* 印刷用のスタイル */
                @media print {
                    /* Streamlitのヘッダーや不要なUI、タイトル等を非表示にする */
                    header, footer, [data-testid="stHeader"], [data-testid="stToolbar"], .no-print {
                        display: none !important;
                    }
                    /* 停留所選択メニューや印刷ボタン(iframe)自体を非表示にする */
                    iframe, [data-testid="stSelectbox"] {
                        display: none !important;
                    }
                    /* 印刷時の余白を調整 */
                    @page {
                        margin: 10mm;
                    }
                }
                </style>
                """, unsafe_allow_html=True)

                # HTMLタグを解釈させるため、データフレームをHTMLに変換して描画（インデックス列は非表示）
                html_table = combined_df.to_html(escape=False, index=False)
                st.markdown(html_table, unsafe_allow_html=True)

                # 印刷ボタン（Reactのエラーを防ぐためiframe内で描画し、親要素を印刷させる）
                components.html("""
                <div style="text-align: right; margin-bottom: 5px;">
                    <button onclick="window.parent.print()" style="padding: 0.5rem 1rem; background-color: #0068c9; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 1rem; font-weight: bold; font-family: sans-serif;">
                        🖨️ 時刻表を印刷する
                    </button>
                </div>
                """, height=55)
