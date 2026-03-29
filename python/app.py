import streamlit as st
import pandas as pd
import streamlit.components.v1 as components
import os
from datetime import datetime
import pytz

st.set_page_config(page_title="バス区間時刻表アプリ", layout="wide")

# --- データの読み込み ---
DATA_DIR = "gtfs_data"

st.markdown("""
<div class="no-print">
    <h1 style="font-size: 2.25rem; font-weight: 600; padding-bottom: 1rem;">🚌 総合 区間時刻表アプリ</h1>
    <p style="margin-top: -10px; margin-bottom: 20px;">複数のバス事業者のデータを統合し、同じ停留所名であれば一つの時刻表にまとめて表示します。</p>
</div>
""", unsafe_allow_html=True)

# データの読み込み (キャッシュ機能を使って2回目以降の表示を高速化)
@st.cache_data
def load_all_data(data_dir):
    all_stops, all_routes, all_trips, all_stop_times, all_calendars, all_calendar_dates = [], [], [], [], [], []
    if not os.path.exists(data_dir): return (None,) * 6
    
    for operator in os.listdir(data_dir):
        op_path = os.path.join(data_dir, operator)
        if not os.path.isdir(op_path): continue
        
        try:
            stops = pd.read_csv(os.path.join(op_path, "stops.txt"), dtype=str, encoding='utf-8')
            routes = pd.read_csv(os.path.join(op_path, "routes.txt"), dtype=str, encoding='utf-8')
            trips = pd.read_csv(os.path.join(op_path, "trips.txt"), dtype=str, encoding='utf-8')
            stop_times = pd.read_csv(os.path.join(op_path, "stop_times.txt"), dtype=str, encoding='utf-8')
            calendar = pd.read_csv(os.path.join(op_path, "calendar.txt"), dtype=str, encoding='utf-8')
            
            # IDの衝突を防ぐため、事業者名をプレフィックスとして付与
            prefix = f"{operator}_"
            stops['stop_id'] = prefix + stops['stop_id']
            routes['route_id'] = prefix + routes['route_id']
            
            # 系統名に事業者名を追加
            if 'route_short_name' in routes.columns:
                routes['route_short_name'] = f"[{operator}] " + routes['route_short_name'].str.replace('市バス', '', regex=False)
            else:
                routes['route_short_name'] = f"[{operator}] " + routes['route_long_name']
                
            trips['trip_id'] = prefix + trips['trip_id']
            if 'route_id' in trips.columns: trips['route_id'] = prefix + trips['route_id']
            if 'service_id' in trips.columns: trips['service_id'] = prefix + trips['service_id']
            stop_times['trip_id'] = prefix + stop_times['trip_id']
            stop_times['stop_id'] = prefix + stop_times['stop_id']
            if 'service_id' in calendar.columns: calendar['service_id'] = prefix + calendar['service_id']
            
            all_stops.append(stops); all_routes.append(routes); all_trips.append(trips); all_stop_times.append(stop_times); all_calendars.append(calendar)

            # calendar_dates.txt はオプションファイルなので、なくてもエラーにしない
            try:
                calendar_dates = pd.read_csv(os.path.join(op_path, "calendar_dates.txt"), dtype=str, encoding='utf-8')
                if 'service_id' in calendar_dates.columns: calendar_dates['service_id'] = prefix + calendar_dates['service_id']
                all_calendar_dates.append(calendar_dates)
            except FileNotFoundError:
                pass
        except FileNotFoundError: 
            continue
            
    if not all_stops: return (None,) * 6
    
    dfs_to_concat = [all_stops, all_routes, all_trips, all_stop_times, all_calendars, all_calendar_dates]
    concatenated_dfs = [pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame() for df_list in dfs_to_concat]
    return tuple(concatenated_dfs)

stops, routes, trips, stop_times, calendar, calendar_dates = load_all_data(DATA_DIR)

if stops is None:
    st.error(f"⚠️ GTFSデータが見つかりません。`{DATA_DIR}` フォルダ内に事業者ごとのデータ（stops.txt, routes.txt, trips.txt, stop_times.txt, calendar.txtなど）を配置してください。")
else:
    # 1. 出発・到着停留所の選択UI
    # 欠損値を除外し、あいうえお順にソートしてドロップダウンメニュー化
    stop_names = sorted(stops['stop_name'].dropna().unique())
    
    col1, col2 = st.columns(2)
    with col1:
        # デフォルトの停留所が存在しない場合でもエラーにならないようにする
        dep_index = stop_names.index("京都駅前") if "京都駅前" in stop_names else 0
        selected_dep_stop = st.selectbox("🚏 出発停留所を選択", stop_names, index=dep_index)
    with col2:
        # デフォルトの停留所が存在しない場合でもエラーにならないようにする
        arr_index = stop_names.index("四条河原町") if "四条河原町" in stop_names else 1
        selected_arr_stop = st.selectbox("🚏 到着停留所を選択", stop_names, index=arr_index)

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

                # --- 次の便をハイライトするための準備 ---
                next_bus_index = None
                today_type = None
                try:
                    jst = pytz.timezone('Asia/Tokyo')
                    now = datetime.now(jst)
                    current_time_str = now.strftime('%H:%M:%S')
                    day_of_week = now.weekday() # Monday=0, Sunday=6
                    
                    if day_of_week == 5:
                        today_type = '土曜'
                    elif day_of_week == 6:
                        today_type = '休日'
                    else:
                        today_type = '平日'
                except Exception:
                    # pytzがない場合などでもアプリが停止しないようにする
                    current_time_str = "00:00:00"
                    pass
                # --- ここまで ---

                st.subheader(f"時刻表: {selected_dep_stop} ➔ {selected_arr_stop} ")

                # calendar.txtからダイヤIDと「平日」「土曜」「休日」の対応付けを作成
                service_mapping = {}
                temporary_services = set()
                for _, row in calendar.iterrows():
                    if row.get('monday') == '1' or row.get('tuesday') == '1' or row.get('wednesday') == '1' or row.get('thursday') == '1' or row.get('friday') == '1':
                        service_mapping[row['service_id']] = '平日'
                    elif row.get('saturday') == '1':
                        service_mapping[row['service_id']] = '土曜'
                    elif row.get('sunday') == '1':
                        service_mapping[row['service_id']] = '休日'
                
                # calendar.txtで定義されていない臨時ダイヤ等をcalendar_dates.txtから推測
                unmapped_services = set(timetable['service_id'].unique()) - set(service_mapping.keys())
                if not calendar_dates.empty and unmapped_services:
                    calendar_dates['date_dt'] = pd.to_datetime(calendar_dates['date'], format='%Y%m%d')
                    for service_id in unmapped_services:
                        # 運行が追加される(exception_type=1)最初の日付を探す
                        runs = calendar_dates[(calendar_dates['service_id'] == service_id) & (calendar_dates['exception_type'] == '1')]
                        if runs.empty: continue
                        
                        first_run = runs.sort_values('date_dt').iloc[0]
                        day_of_week = first_run['date_dt'].dayofweek # Monday=0, Sunday=6
                        
                        if day_of_week == 5: # Saturday
                            service_mapping[service_id] = '土曜'
                        elif day_of_week == 6: # Sunday
                            service_mapping[service_id] = '休日'
                        else: # Weekday
                            service_mapping[service_id] = '平日'
                        temporary_services.add(service_id)

                # service_idを曜日名に変換し、複数事業者の同じ曜日（平日など）をまとめる
                timetable['day_name'] = timetable['service_id'].map(service_mapping).fillna(timetable['service_id'])
                
                # --- 次の便のインデックスを特定 ---
                if today_type and today_type in timetable['day_name'].values:
                    today_timetable = timetable[timetable['day_name'] == today_type]
                    next_bus_candidates = today_timetable[today_timetable['departure_time'] > current_time_str]
                    if not next_bus_candidates.empty:
                        next_bus_index = next_bus_candidates.index[0]
                # --- ここまで ---
                
                display_days = []
                for d in ['平日', '土曜', '休日']:
                    if d in timetable['day_name'].values: display_days.append(d)
                for d in sorted(timetable['day_name'].unique()):
                    if d not in display_days: display_days.append(d)

                # 凡例を作成（系統ごとに ①, ②, ③... などの記号を割り当て）
                unique_routes = sorted(timetable['route_short_name'].unique())
                route_symbols = {route: chr(9312 + i) if i < 20 else f"[{i+1}]" for i, route in enumerate(unique_routes)}
                
                # 臨時ダイヤに該当する系統名を特定
                temp_route_names = set(timetable[timetable['service_id'].isin(temporary_services)]['route_short_name'].unique())
                
                legend_items = [f"{symbol}: {route}系統{'*' if route in temp_route_names else ''}" for route, symbol in route_symbols.items()]
                st.markdown("**凡例:**　" + " ｜ ".join(legend_items))
                if temp_route_names:
                    st.markdown("<small>（* は期間・曜日限定で運行される臨時ダイヤの可能性があります）</small>", unsafe_allow_html=True)

                schedule_data = {}
                
                for day in display_days:
                    df_day = timetable[timetable['day_name'] == day].copy()
                    
                    # 時刻を「時」と「分」に分割
                    df_day['hour'] = df_day['departure_time'].str.split(':').str[0].astype(int)
                    
                    # --- HTML生成ロジックの変更 ---
                    def create_minute_html(row):
                        minute_str = row['departure_time'].split(':')[1]
                        symbol_str = route_symbols.get(row['route_short_name'], '◆') # 安全のため.getを使用
                        
                        base_html = f"<span style='font-size: 1.25em; font-weight: 500;'>{minute_str}</span><span style='font-size: 0.85em;'>{symbol_str}</span>"
                        
                        # この行が「次の便」であればハイライト用クラスを適用
                        if row.name == next_bus_index:
                            return f"<span class='next-bus-highlight'>{base_html}</span>"
                        return base_html

                    df_day['minute_display'] = df_day.apply(create_minute_html, axis=1)
                    # --- ここまで ---
                    
                    # 時間(hour)ごとにグループ化してテキストを結合
                    schedule_data[day] = df_day.groupby('hour')['minute_display'].apply(lambda x: "　".join(x))
                
                # すべてのダイヤを1つのデータフレームに結合（無い時間帯のNaNは空文字で埋める）
                combined_df = pd.DataFrame(schedule_data).fillna("")
                
                # display_days の順番で列を並び替え
                combined_df = combined_df[display_days]

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
                /* 土曜・休日のヘッダー色 */
                table.dataframe th.saturday-header {
                    background-color: rgba(0, 104, 201, 0.2);
                }
                table.dataframe th.holiday-header {
                    background-color: rgba(255, 71, 71, 0.2);
                }
                /* 次の便のハイライト */
                .next-bus-highlight {
                    background-color: rgba(255, 255, 0, 0.6);
                    border-radius: 4px;
                    padding: 2px 4px;
                    display: inline-block;
                }
                /* 印刷用のスタイル */
                @media print {
                    /* Streamlitのヘッダーや不要なUI、タイトル等を非表示にする */
                    header, footer, [data-testid="stHeader"], [data-testid="stToolbar"], .no-print {
                        display: none !important;
                    }
                    /* 停留所選択メニュー(水平ブロック)や印刷ボタンを含むコンテナごと非表示にする */
                    [data-testid="stHorizontalBlock"],
                    .element-container:has(iframe) {
                        display: none !important;
                    }
                    /* Streamlitが自動付与するページ上下左右の余白を消す */
                    .block-container {
                        padding: 0 !important;
                        margin: 0 !important;
                        max-width: 100% !important;
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
                html_table = html_table.replace('<th>土曜</th>', '<th class="saturday-header">土曜</th>', 1)
                html_table = html_table.replace('<th>休日</th>', '<th class="holiday-header">休日</th>', 1)
                st.markdown(html_table, unsafe_allow_html=True)

                # 印刷ボタン（Reactのエラーを防ぐためiframe内で描画し、親要素を印刷させる）
                components.html("""
                <div style="text-align: right; margin-bottom: 5px;">
                    <button onclick="window.parent.print()" style="padding: 0.5rem 1rem; background-color: #0068c9; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 1rem; font-weight: bold; font-family: sans-serif;">
                        🖨️ 時刻表を印刷する
                    </button>
                </div>
                """, height=55)
