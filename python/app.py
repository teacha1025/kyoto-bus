import streamlit as st
import pandas as pd
import streamlit.components.v1 as components
import os
from datetime import datetime
import pydeck as pdk
import pytz
from urllib.parse import quote
import numpy as np
from streamlit_geolocation import streamlit_geolocation
import requests
import zipfile
import io

st.set_page_config(page_title="バス区間時刻表アプリ", layout="wide")

# --- データの読み込み ---
DATA_DIR = "gtfs_data"

# --- GTFSデータの自動準備 ---
# ダウンロードURLがわかる場合はここに記載します
GTFS_URLS = {
    "kyoto_city": "https://api.odpt.org/api/v4/files/odpt/KyotoMunicipalTransportation/Kyoto_City_Bus_GTFS.zip?date=20260323",
    "kyoto_bus": "https://api.odpt.org/api/v4/files/odpt/KyotoBus/AllLines.zip?date=20260328"
}

@st.cache_resource
def prepare_gtfs_data(data_dir):
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        
    # StreamlitのSecretsからトークンを取得（設定されていない場合はNone）
    odpt_token = st.secrets.get("ODPT_TOKEN")
    
    for operator, url in GTFS_URLS.items():
        op_path = os.path.join(data_dir, operator)
        if not os.path.exists(op_path) or not os.listdir(op_path):
            try:
                params = {}
                # URLにODPTが含まれていて、トークンが設定されている場合はパラメータに付与
                if odpt_token and "odpt" in url.lower():
                    params["acl:consumerKey"] = odpt_token
                    
                response = requests.get(url, params=params)
                response.raise_for_status()
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    z.extractall(op_path)
                print(f"✅ {operator} のデータ取得に成功しました")
            except Exception as e:
                print(f"❌ {operator} のデータ取得に失敗しました: {e}")
                
prepare_gtfs_data(DATA_DIR)

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
    operator_expirations = {}
    if not os.path.exists(data_dir): return (None,) * 6, {}
    
    for operator in os.listdir(data_dir):
        op_path = os.path.join(data_dir, operator)
        if not os.path.isdir(op_path): continue
        
        try:
            stops = pd.read_csv(os.path.join(op_path, "stops.txt"), dtype=str, encoding='utf-8')
            routes = pd.read_csv(os.path.join(op_path, "routes.txt"), dtype=str, encoding='utf-8')
            trips = pd.read_csv(os.path.join(op_path, "trips.txt"), dtype=str, encoding='utf-8')
            stop_times = pd.read_csv(os.path.join(op_path, "stop_times.txt"), dtype=str, encoding='utf-8')
            calendar = pd.read_csv(os.path.join(op_path, "calendar.txt"), dtype=str, encoding='utf-8')
            
            # 期限の取得 (feed_info.txt が存在する場合)
            try:
                feed_info = pd.read_csv(os.path.join(op_path, "feed_info.txt"), dtype=str, encoding='utf-8')
                if 'feed_end_date' in feed_info.columns and not feed_info['feed_end_date'].isna().all():
                    operator_expirations[operator] = feed_info['feed_end_date'].dropna().iloc[0]
            except Exception:
                pass

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
            
    if not all_stops: return (None,) * 6, {}
    
    dfs_to_concat = [all_stops, all_routes, all_trips, all_stop_times, all_calendars, all_calendar_dates]
    concatenated_dfs = [pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame() for df_list in dfs_to_concat]
    return tuple(concatenated_dfs), operator_expirations

data_dfs, operator_expirations = load_all_data(DATA_DIR)
stops, routes, trips, stop_times, calendar, calendar_dates = data_dfs

# --- 有効期限のチェックと警告表示 ---
if stops is not None:
    expired_ops = []
    current_date_str = datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y%m%d')
    for op, end_date in operator_expirations.items():
        if end_date < current_date_str:
            formatted_date = f"{end_date[:4]}/{end_date[4:6]}/{end_date[6:]}" if len(end_date) == 8 else end_date
            expired_ops.append(f"**{op}** (期限: {formatted_date})")
            
    if expired_ops:
        st.warning(f"⚠️ 以下の事業者の時刻表データは有効期限が切れています。新しいGTFSデータをダウンロードし、`{DATA_DIR}` フォルダ内を更新してください：\n\n" + " / ".join(expired_ops))

if stops is None:
    st.error(f"⚠️ GTFSデータが見つかりません。`{DATA_DIR}` フォルダ内に事業者ごとのデータ（stops.txt, routes.txt, trips.txt, stop_times.txt, calendar.txtなど）を配置してください。")
elif "trip_id" in st.query_params:
    trip_id = st.query_params["trip_id"]
    
    if st.button("⬅️ 時刻表に戻る"):
        params_to_keep = {k: v for k, v in st.query_params.items() if k not in ['trip_id', 'stop_seq']}
        st.query_params.clear()
        st.query_params.update(params_to_keep)
        st.rerun()

    trip_info = trips[trips['trip_id'] == trip_id]

    if trip_info.empty:
        st.error("指定された便の情報が見つかりません。")
    else:
        route_id = trip_info.iloc[0]['route_id']
        route_info = routes[routes['route_id'] == route_id]
        route_name = route_info.iloc[0]['route_short_name'] if not route_info.empty else "不明な系統"
        headsign = trip_info.iloc[0]['trip_headsign'] if pd.notna(trip_info.iloc[0]['trip_headsign']) else "不明"
        
        st.subheader(f"🚌 {route_name}系統 ({headsign} 行き) 詳細情報")
        
        st_times = stop_times[stop_times['trip_id'] == trip_id].copy()
        st_times['stop_sequence'] = pd.to_numeric(st_times['stop_sequence'])
        st_times = st_times.sort_values('stop_sequence')
        
        merged_stops = pd.merge(st_times, stops, on='stop_id', how='left')
        
        col1, col2 = st.columns([1, 1])
        
        # col2 (右側) を先に処理して、テーブルの行選択(クリック)イベントを取得する
        with col2:
            st.markdown("##### 🚏 停車停留所と通過時刻")
            st.markdown("<small>💡 停留所名をクリックすると、地図がその位置へ移動します</small>", unsafe_allow_html=True)
            display_df = merged_stops[['stop_sequence', 'stop_name', 'arrival_time', 'departure_time']].copy()
            display_df.columns = ['順序', '停留所名', '到着', '出発']
            # 順序を1から始まる連番に振り直す
            display_df['順序'] = range(1, len(display_df) + 1)
            # 時刻を「時:分」の形式にして見やすくする（秒を省く）
            display_df['到着'] = display_df['到着'].astype(str).str[:5]
            display_df['出発'] = display_df['出発'].astype(str).str[:5]
            
            # セル（停留所名）をリンクに変換
            def create_stop_link(row):
                seq = row['順序']
                name = row['停留所名']
                dep_encoded = quote(st.query_params.get("dep", ""))
                arr_encoded = quote(st.query_params.get("arr", ""))
                return f"<a href='?dep={dep_encoded}&arr={arr_encoded}&trip_id={trip_id}&stop_seq={seq}' target='_self' class='stop-link'>{name}</a>"
                
            display_df['停留所名'] = display_df.apply(create_stop_link, axis=1)

            st.markdown("""
            <style>
            table.stop-table {
                width: 100%;
                border-collapse: collapse;
                text-align: center;
                font-size: 0.95rem;
            }
            table.stop-table th, table.stop-table td {
                border: 1px solid var(--border-color, #e0e0e0);
                padding: 0.5rem;
            }
            table.stop-table th {
                background-color: var(--secondary-background-color, #f0f2f6);
            }
            .stop-link {
                text-decoration: none !important;
                color: inherit !important;
                display: block;
                margin: -0.5rem; /* 親セルのパディングを埋める */
                padding: 0.5rem; /* パディングを再適用してテキスト位置を維持 */
                transition: background-color 0.2s;
            }
            .stop-link:hover {
                background-color: rgba(128, 128, 128, 0.15); /* ホバー時に背景色を変更 */
                text-decoration: none !important; /* 下線は表示しない */
            }
            </style>
            """, unsafe_allow_html=True)
            
            # HTMLテーブルとして描画
            st.markdown(display_df.to_html(escape=False, index=False, classes="stop-table"), unsafe_allow_html=True)

            # URLパラメータから選択された行を取得
            selected_seq = st.query_params.get("stop_seq")
            selected_row = int(selected_seq) - 1 if selected_seq and selected_seq.isdigit() else None

        with col1:
            st.markdown("##### 📍 運行ルート（停留所位置）")
            
            # 現在地を取得するためのウィジェット
            loc = streamlit_geolocation()
            user_lat, user_lon = None, None
            if loc and loc.get('latitude') is not None and loc.get('longitude') is not None:
                user_lat = float(loc['latitude'])
                user_lon = float(loc['longitude'])

            if 'stop_lat' in merged_stops.columns and 'stop_lon' in merged_stops.columns:
                map_data = merged_stops[['stop_lat', 'stop_lon', 'stop_name', 'arrival_time', 'departure_time']].copy()
                map_data['lat'] = pd.to_numeric(map_data['stop_lat'], errors='coerce')
                map_data['lon'] = pd.to_numeric(map_data['stop_lon'], errors='coerce')
                map_data = map_data.dropna(subset=['lat', 'lon'])
                # ツールチップ表示用に時刻を整形
                map_data['arrival_time'] = map_data['arrival_time'].astype(str).str[:5]
                map_data['departure_time'] = map_data['departure_time'].astype(str).str[:5]
                
                if not map_data.empty:
                    # マーカーの色とサイズを初期化
                    map_data['r'] = 255
                    map_data['g'] = 0
                    map_data['b'] = 0
                    map_data['a'] = 180
                    map_data['radius'] = 8
                    
                    selected_lat, selected_lon = None, None
                    if selected_row is not None and selected_row in map_data.index:
                        # 選択された停留所をハイライト（黄色）
                        map_data.at[selected_row, 'g'] = 200
                        map_data.at[selected_row, 'a'] = 255
                        
                        selected_lat = map_data.at[selected_row, 'lat']
                        selected_lon = map_data.at[selected_row, 'lon']
                        
                    # 地図の中心とズームレベルを計算
                    if selected_lat is not None and selected_lon is not None:
                        mid_lat = selected_lat
                        mid_lon = selected_lon
                        zoom_level = 15
                    else:
                        mid_lat = map_data['lat'].mean()
                        mid_lon = map_data['lon'].mean()
                        zoom_level = 12

                    # Pydeckを使用して地図を描画
                    view_state = pdk.ViewState(
                        latitude=mid_lat,
                        longitude=mid_lon,
                        zoom=zoom_level,
                        pitch=0,
                    )

                    layer = pdk.Layer(
                        "ScatterplotLayer",
                        data=map_data,
                        get_position="[lon, lat]",
                        get_radius="radius",  # 半径を列から取得
                        radius_units='meters',
                        get_fill_color="[r, g, b, a]", # 色を列から取得
                        pickable=True,
                        auto_highlight=True,
                    )

                    tooltip = {
                        "html": "<b>{stop_name}</b><br/>到着: {arrival_time}<br/>出発: {departure_time}",
                        "style": {
                            "backgroundColor": "steelblue",
                            "color": "white",
                            "font-family": "sans-serif",
                            "font-size": "0.8rem",
                        }
                    }

                    pydeck_map_style = "light"

                    layers = [layer]
                    
                    # 現在地のマーカー（青色）を追加
                    if user_lat and user_lon:
                        user_data = pd.DataFrame([{
                            "lat": user_lat, 
                            "lon": user_lon, 
                            "stop_name": "📍 現在地", 
                            "arrival_time": "---", 
                            "departure_time": "---"
                        }])
                        user_layer = pdk.Layer(
                            "ScatterplotLayer",
                            data=user_data,
                            get_position="[lon, lat]",
                            get_radius=8,
                            radius_units='meters',
                            get_fill_color=[0, 100, 255, 200],  # 青色
                            pickable=True,
                            auto_highlight=True,
                        )
                        layers.append(user_layer)

                    st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view_state, tooltip=tooltip, map_style=pydeck_map_style))
                else:
                    st.warning("位置情報データがありません。")
            else:
                st.warning("位置情報データがありません。")

else:
    # 1. 出発・到着停留所の選択UI
    # 欠損値を除外し、あいうえお順にソートしてドロップダウンメニュー化
    stop_names = sorted(stops['stop_name'].dropna().unique())

    # --- URLパラメータとウィジェットの状態を同期 ---
    dep_param = st.query_params.get("dep")
    arr_param = st.query_params.get("arr")
    
    try:
        dep_index = stop_names.index(dep_param) if dep_param else (stop_names.index("京都駅前") if "京都駅前" in stop_names else 0)
    except ValueError:
        dep_index = 0
        
    try:
        arr_index = stop_names.index(arr_param) if arr_param else (stop_names.index("四条河原町") if "四条河原町" in stop_names else 1)
    except ValueError:
        arr_index = 1

    col1, col2, col3 = st.columns([5, 5, 2])
    with col1:
        selected_dep_stop = st.selectbox("🚏 出発停留所を選択", stop_names, index=dep_index)
    with col2:
        selected_arr_stop = st.selectbox("🚏 到着停留所を選択", stop_names, index=arr_index)
        
    # 選択をURLパラメータに反映（変更があれば自動で再実行される）
    st.query_params["dep"] = selected_dep_stop
    st.query_params["arr"] = selected_arr_stop

    with col3:
        # ボタンを垂直方向に中央揃えするためのプレースホルダー
        st.markdown("<div style='height: 28px'></div>", unsafe_allow_html=True)
        components.html("""
            <style>
                body { margin: 0; font-family: sans-serif; }
                button { width: 100%; height: 40px; background-color: #fff; color: #0068c9; border: 1px solid #0068c9; border-radius: 0.5rem; cursor: pointer; font-size: 1rem; font-weight: bold; transition: all 0.2s; }
                button:hover { background-color: #f0f2f6; }
            </style>
            <button id="share-btn">🔗 共有</button>
            <script>
                const btn = document.getElementById('share-btn');
                btn.onclick = () => {
                    navigator.clipboard.writeText(window.parent.location.href).then(() => {
                        btn.innerText = '✅ コピー済';
                        setTimeout(() => { btn.innerText = '🔗 共有'; }, 2000);
                    }).catch(() => {
                        btn.innerText = '⚠️ 失敗';
                        setTimeout(() => { btn.innerText = '🔗 共有'; }, 2000);
                    });
                };
            </script>
        """, height=45)
    
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
                timetable = valid_st[['trip_id', 'departure_time', 'route_short_name', 'trip_headsign', 'service_id']].copy()
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
                        
                        trip_id_val = row['trip_id']
                        # URLエンコードした現在の停留所選択を維持しつつ、trip_idを追加
                        dep_encoded = quote(st.query_params.get("dep", ""))
                        arr_encoded = quote(st.query_params.get("arr", ""))
                        link_html = f"<a href='?dep={dep_encoded}&arr={arr_encoded}&trip_id={trip_id_val}' target='_self' class='trip-link'>{base_html}</a>"
                        
                        # この行が「次の便」であればハイライト用クラスを適用
                        if row.name == next_bus_index:
                            return f"<span class='next-bus-highlight'>{link_html}</span>"
                        return link_html

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
                /* リンクのホバー効果 */
                .trip-link {
                text-decoration: none !important;
                color: inherit !important;
                    display: inline-block;
                    padding: 0.2rem 0.3rem;
                    border-radius: 4px;
                    transition: background-color 0.2s;
                }
                .trip-link:hover {
                    background-color: rgba(128, 128, 128, 0.15);
                text-decoration: none !important;
                }
                /* 印刷用のスタイル */
                @media print {
                    /* 次の便のハイライトを無効化 */
                    .next-bus-highlight {
                        background-color: transparent !important;
                        padding: 0 !important;
                    }
                    /* Streamlitのヘッダーや不要なUI、タイトル等を非表示にする */
                    header, footer, [data-testid="stHeader"], [data-testid="stToolbar"], .no-print {
                        display: none !important;
                    }
                    /* 停留所選択メニュー(水平ブロック)や印刷ボタンを含むコンテナごと非表示にする */
                    [data-testid="stHorizontalBlock"],
                    [data-testid="stCheckbox"],
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
                <style>body { background-color: transparent !important; margin: 0; }</style>
                <div style="text-align: right; margin-bottom: 5px;">
                    <button onclick="window.parent.print()" style="padding: 0.5rem 1rem; background-color: #0068c9; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 1rem; font-weight: bold; font-family: sans-serif;">
                        🖨️ 時刻表を印刷する
                    </button>
                </div>
                """, height=55)
