import os
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

try:
    import plotly.express as px
except ImportError:  # pragma: no cover - shown in the UI when dependency is missing
    px = None


BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR / "spotify_data_clean.parquet"
SPARK_HOME = BASE_DIR / "spark-3.5.8-bin-hadoop3"

ARTIST_COL = "Artist(s)"
SONG_COL = "song"
GENRE_COL = "Genre"
EMOTION_COL = "emotion"
DATE_COL = "Release Date"
EXPLICIT_COL = "Explicit"
POPULARITY_COL = "Popularity"
POSITIVENESS_COL = "Positiveness"

NUMERIC_COLUMNS = [
    "Popularity",
    "Energy",
    "Danceability",
    "Positiveness",
    "Speechiness",
    "Liveness",
    "Acousticness",
    "Instrumentalness",
    "Tempo",
    "Good for Party",
    "Good for Work/Study",
    "Good for Relaxation/Meditation",
    "Good for Exercise",
    "Good for Running",
    "Good for Yoga/Stretching",
    "Good for Driving",
    "Good for Social Gatherings",
    "Good for Morning Routine",
]

AUDIO_FEATURES = [
    "Energy_num",
    "Danceability_num",
    "Positiveness_num",
    "Speechiness_num",
    "Liveness_num",
    "Acousticness_num",
    "Instrumentalness_num",
]

DISPLAY_COLUMNS = [
    ARTIST_COL,
    SONG_COL,
    GENRE_COL,
    EMOTION_COL,
    DATE_COL,
    "release_year",
    EXPLICIT_COL,
    "Popularity_num",
    "Energy_num",
    "Danceability_num",
    "Positiveness_num",
    "sentiment",
]


st.set_page_config(
    page_title="Spotify Sentiment Dashboard",
    page_icon=":musical_note:",
    layout="wide",
    initial_sidebar_state="expanded",
)


st.markdown(
    """
    <style>
    .block-container {padding-top: 1.25rem; padding-bottom: 2rem;}
    [data-testid="stMetricValue"] {font-size: 1.75rem;}
    div[data-testid="stButton"] > button {
        width: 100%;
        border-radius: 8px;
        min-height: 2.35rem;
        font-weight: 600;
    }
    div[data-testid="stDownloadButton"] > button {
        width: 100%;
        border-radius: 8px;
        min-height: 2.35rem;
        font-weight: 600;
    }
    .section-note {color: #586174; font-size: 0.92rem;}
    </style>
    """,
    unsafe_allow_html=True,
)


def require_plotly() -> None:
    if px is None:
        st.error("Dashboard cần thư viện plotly. Cài bằng: pip install plotly")
        st.stop()


@st.cache_resource(show_spinner=False)
def get_spark() -> SparkSession:
    os.environ.setdefault("SPARK_HOME", str(SPARK_HOME))
    os.environ.setdefault("PYSPARK_PYTHON", str(BASE_DIR / "myenv" / "bin" / "python"))
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    return (
        SparkSession.builder.master("local[*]")
        .appName("spotify-local-dashboard")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.columnarReaderBatchSize", "512")
        .getOrCreate()
    )


def numeric_value(column_name: str):
    return F.regexp_extract(F.col(column_name).cast("string"), r"-?\d+(?:\.\d+)?", 0).cast(DoubleType())


@st.cache_resource(show_spinner="Đang đọc dữ liệu parquet bằng Spark...")
def load_prepared_data():
    spark = get_spark()
    raw_columns = [
        ARTIST_COL,
        SONG_COL,
        GENRE_COL,
        EMOTION_COL,
        DATE_COL,
        EXPLICIT_COL,
        *NUMERIC_COLUMNS,
    ]
    available_columns = spark.read.parquet(str(DATA_PATH)).columns
    selected_columns = [column_name for column_name in dict.fromkeys(raw_columns) if column_name in available_columns]
    df = spark.read.parquet(str(DATA_PATH)).select(*selected_columns)

    for column_name in NUMERIC_COLUMNS:
        if column_name in df.columns:
            df = df.withColumn(f"{column_name}_num", numeric_value(column_name))

    df = df.withColumn("release_year", F.regexp_extract(F.col(DATE_COL).cast("string"), r"(\d{4})", 1).cast("int"))
    df = df.withColumn(
        "sentiment",
        F.when(F.col("Positiveness_num") >= 50, F.lit("Positive")).otherwise(F.lit("Negative")),
    )
    df = df.withColumn("is_explicit", F.lower(F.col(EXPLICIT_COL).cast("string")) == F.lit("yes"))
    df = df.cache()
    df.count()
    return df


def to_pandas(spark_df, limit: Optional[int] = None) -> pd.DataFrame:
    if limit is not None:
        spark_df = spark_df.limit(limit)
    return spark_df.toPandas()


def reset_filters() -> None:
    st.session_state["genre_filter"] = []
    st.session_state["emotion_filter"] = []
    st.session_state["sentiment_filter"] = []
    st.session_state["explicit_filter"] = "All"
    st.session_state["artist_search"] = ""
    st.session_state["song_search"] = ""
    st.session_state.pop("prediction_song", None)
    st.session_state["_reset_range_filters"] = True


def build_sidebar(df):
    st.sidebar.title("Bộ lọc theo dõi")

    if st.sidebar.button("Làm mới dữ liệu", help="Xóa cache và đọc lại parquet"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

    st.sidebar.button(
        "Xóa toàn bộ lọc",
        help="Đưa dashboard về trạng thái mặc định",
        on_click=reset_filters,
    )

    st.sidebar.divider()

    genres = [
        row[GENRE_COL]
        for row in (
            df.groupBy(GENRE_COL)
            .agg(F.count("*").alias("tracks"))
            .where(F.col(GENRE_COL).isNotNull() & (F.length(F.col(GENRE_COL)) <= 40))
            .orderBy(F.desc("tracks"), GENRE_COL)
            .limit(80)
            .collect()
        )
        if row[GENRE_COL]
    ]
    emotions = [
        row[EMOTION_COL]
        for row in (
            df.groupBy(EMOTION_COL)
            .agg(F.count("*").alias("tracks"))
            .where(F.col(EMOTION_COL).isNotNull() & (F.length(F.col(EMOTION_COL)) <= 30))
            .orderBy(F.desc("tracks"), EMOTION_COL)
            .limit(40)
            .collect()
        )
        if row[EMOTION_COL]
    ]

    selected_genres = st.sidebar.multiselect("Genre", genres, key="genre_filter")
    selected_emotions = st.sidebar.multiselect("Emotion", emotions, key="emotion_filter")
    selected_sentiment = st.sidebar.multiselect(
        "Sentiment",
        ["Positive", "Negative"],
        default=st.session_state.get("sentiment_filter", []),
        key="sentiment_filter",
    )
    explicit = st.sidebar.selectbox(
        "Explicit",
        ["All", "Yes", "No"],
        index=["All", "Yes", "No"].index(st.session_state.get("explicit_filter", "All")),
        key="explicit_filter",
    )

    bounds = df.select(
        F.min("release_year").alias("min_year"),
        F.max("release_year").alias("max_year"),
        F.min("Popularity_num").alias("min_pop"),
        F.max("Popularity_num").alias("max_pop"),
    ).collect()[0]

    min_year = max(1900, int(bounds["min_year"] or 1900))
    max_year = min(2026, int(bounds["max_year"] or 2026))
    if min_year > max_year:
        min_year, max_year = 1900, 2026
    min_pop = 0
    max_pop = 100

    if st.session_state.pop("_reset_range_filters", False):
        st.session_state["year_filter"] = (min_year, max_year)
        st.session_state["popularity_filter"] = (min_pop, max_pop)

    years = st.sidebar.slider(
        "Năm phát hành",
        min_value=min_year,
        max_value=max_year,
        value=st.session_state.get("year_filter", (min_year, max_year)),
        key="year_filter",
    )
    popularity = st.sidebar.slider(
        "Popularity",
        min_value=min_pop,
        max_value=max_pop,
        value=st.session_state.get("popularity_filter", (min_pop, max_pop)),
        key="popularity_filter",
    )

    artist_search = st.sidebar.text_input("Tìm nghệ sĩ", key="artist_search")
    song_search = st.sidebar.text_input("Tìm bài hát", key="song_search")

    return {
        "years": years,
        "popularity": popularity,
        "genres": selected_genres,
        "emotions": selected_emotions,
        "sentiment": selected_sentiment,
        "explicit": explicit,
        "artist_search": artist_search,
        "song_search": song_search,
    }


def apply_filters(df, filters):
    filtered = df.where(F.col("release_year").between(filters["years"][0], filters["years"][1]))
    filtered = filtered.where(F.col("Popularity_num").between(filters["popularity"][0], filters["popularity"][1]))

    if filters["genres"]:
        filtered = filtered.where(F.col(GENRE_COL).isin(filters["genres"]))
    if filters["emotions"]:
        filtered = filtered.where(F.col(EMOTION_COL).isin(filters["emotions"]))
    if filters["sentiment"]:
        filtered = filtered.where(F.col("sentiment").isin(filters["sentiment"]))
    if filters["explicit"] != "All":
        filtered = filtered.where(F.lower(F.col(EXPLICIT_COL)) == filters["explicit"].lower())
    if filters["artist_search"]:
        filtered = filtered.where(F.lower(F.col(ARTIST_COL)).contains(filters["artist_search"].lower()))
    if filters["song_search"]:
        filtered = filtered.where(F.lower(F.col(SONG_COL)).contains(filters["song_search"].lower()))

    return filtered.cache()


def draw_metric_row(df):
    stats = df.agg(
        F.count("*").alias("tracks"),
        F.countDistinct(ARTIST_COL).alias("artists"),
        F.countDistinct(GENRE_COL).alias("genres"),
        F.avg("Popularity_num").alias("avg_popularity"),
        F.avg("Positiveness_num").alias("avg_positiveness"),
        F.sum(F.when(F.col("sentiment") == "Positive", 1).otherwise(0)).alias("positive_tracks"),
    ).collect()[0]

    total = int(stats["tracks"] or 0)
    positive_rate = (float(stats["positive_tracks"] or 0) / total * 100) if total else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Tracks", f"{total:,}")
    c2.metric("Artists", f"{int(stats['artists'] or 0):,}")
    c3.metric("Genres", f"{int(stats['genres'] or 0):,}")
    c4.metric("Avg Popularity", f"{float(stats['avg_popularity'] or 0):.1f}")
    c5.metric("Positive Rate", f"{positive_rate:.1f}%")


def plot_bar(data: pd.DataFrame, x: str, y: str, title: str, color: Optional[str] = None):
    require_plotly()
    fig = px.bar(data, x=x, y=y, color=color, title=title, text_auto=".2s")
    fig.update_layout(height=390, margin=dict(l=10, r=10, t=55, b=10), xaxis_title="", yaxis_title="")
    st.plotly_chart(fig, use_container_width=True)


def plot_line(data: pd.DataFrame, x: str, y: str, title: str):
    require_plotly()
    fig = px.line(data, x=x, y=y, markers=True, title=title)
    fig.update_layout(height=380, margin=dict(l=10, r=10, t=55, b=10), xaxis_title="", yaxis_title="")
    st.plotly_chart(fig, use_container_width=True)


def overview_tab(df):
    left, right = st.columns([1.1, 1])

    yearly = to_pandas(
        df.groupBy("release_year")
        .agg(F.count("*").alias("tracks"))
        .where(F.col("release_year").isNotNull())
        .orderBy("release_year")
    )
    with left:
        plot_line(yearly, "release_year", "tracks", "Số lượng bài hát theo năm")

    genre_top = to_pandas(
        df.groupBy(GENRE_COL)
        .agg(F.count("*").alias("tracks"), F.avg("Popularity_num").alias("avg_popularity"))
        .where(F.col(GENRE_COL).isNotNull())
        .orderBy(F.desc("tracks"))
        .limit(12)
    )
    with right:
        plot_bar(genre_top, GENRE_COL, "tracks", "Top genre theo số bài")

    artist_top = to_pandas(
        df.groupBy(ARTIST_COL)
        .agg(F.count("*").alias("tracks"), F.avg("Popularity_num").alias("avg_popularity"))
        .where(F.col(ARTIST_COL).isNotNull())
        .orderBy(F.desc("tracks"))
        .limit(15)
    )
    plot_bar(artist_top, ARTIST_COL, "tracks", "Top nghệ sĩ xuất hiện nhiều nhất")


def sentiment_tab(df):
    left, right = st.columns(2)

    sentiment = to_pandas(df.groupBy("sentiment").agg(F.count("*").alias("tracks")).orderBy("sentiment"))
    with left:
        require_plotly()
        fig = px.pie(sentiment, names="sentiment", values="tracks", hole=0.45, title="Tỉ lệ Positive / Negative")
        fig.update_layout(height=390, margin=dict(l=10, r=10, t=55, b=10))
        st.plotly_chart(fig, use_container_width=True)

    emotion = to_pandas(
        df.groupBy(EMOTION_COL, "sentiment")
        .agg(F.count("*").alias("tracks"))
        .where(F.col(EMOTION_COL).isNotNull())
        .orderBy(F.desc("tracks"))
        .limit(30)
    )
    with right:
        plot_bar(emotion, EMOTION_COL, "tracks", "Emotion theo sentiment", color="sentiment")

    pop_by_sentiment = to_pandas(
        df.groupBy("sentiment").agg(
            F.avg("Popularity_num").alias("avg_popularity"),
            F.avg("Energy_num").alias("avg_energy"),
            F.avg("Danceability_num").alias("avg_danceability"),
            F.avg("Positiveness_num").alias("avg_positiveness"),
        )
    )
    st.dataframe(pop_by_sentiment, use_container_width=True, hide_index=True)


def audio_tab(df):
    feature_summary = to_pandas(
        df.agg(
            *[
                F.avg(feature).alias(feature.replace("_num", ""))
                for feature in AUDIO_FEATURES
                if feature in df.columns
            ]
        )
    ).melt(var_name="feature", value_name="average")
    plot_bar(feature_summary, "feature", "average", "Trung bình các đặc trưng âm thanh")

    scatter = to_pandas(
        df.select(
            ARTIST_COL,
            SONG_COL,
            GENRE_COL,
            "sentiment",
            "Energy_num",
            "Danceability_num",
            "Positiveness_num",
            "Popularity_num",
        )
        .where(
            F.col("Energy_num").isNotNull()
            & F.col("Danceability_num").isNotNull()
            & F.col("Popularity_num").isNotNull()
        )
        .orderBy(F.rand(seed=42))
        .limit(3500)
    )
    require_plotly()
    fig = px.scatter(
        scatter,
        x="Energy_num",
        y="Danceability_num",
        color="sentiment",
        size="Popularity_num",
        hover_data=[ARTIST_COL, SONG_COL, GENRE_COL, "Positiveness_num"],
        title="Energy vs Danceability",
        opacity=0.68,
    )
    fig.update_layout(height=520, margin=dict(l=10, r=10, t=55, b=10), xaxis_title="Energy", yaxis_title="Danceability")
    st.plotly_chart(fig, use_container_width=True)


def prediction_tab(df):
    candidates = to_pandas(
        df.select(
            ARTIST_COL,
            SONG_COL,
            GENRE_COL,
            EMOTION_COL,
            DATE_COL,
            "release_year",
            "Popularity_num",
            "Energy_num",
            "Danceability_num",
            "Positiveness_num",
            "sentiment",
        )
        .where(
            F.col(SONG_COL).isNotNull()
            & F.col(ARTIST_COL).isNotNull()
            & F.col("Positiveness_num").between(0, 100)
        )
        .orderBy(F.desc("Popularity_num"), ARTIST_COL, SONG_COL)
        .limit(2500)
    )

    if candidates.empty:
        st.info("Không có bài hát phù hợp với bộ lọc hiện tại.")
        return

    candidates["choice_label"] = (
        candidates[SONG_COL].fillna("Unknown song")
        + " - "
        + candidates[ARTIST_COL].fillna("Unknown artist")
        + " | "
        + candidates[GENRE_COL].fillna("Unknown genre")
    )
    candidates["choice_label"] = [
        f"{index + 1}. {label}" for index, label in enumerate(candidates["choice_label"].tolist())
    ]

    selected_label = st.selectbox(
        "Chọn bài hát để dự đoán",
        candidates["choice_label"].tolist(),
        key="prediction_song",
    )
    selected = candidates.loc[candidates["choice_label"] == selected_label].iloc[0]

    raw_positiveness = selected["Positiveness_num"]
    positive_percent = float(raw_positiveness) if pd.notna(raw_positiveness) else 0.0
    positive_percent = max(0.0, min(100.0, positive_percent))
    negative_percent = 100.0 - positive_percent
    predicted_label = "Positive" if positive_percent >= negative_percent else "Negative"

    st.subheader(selected[SONG_COL])
    st.caption(
        f"{selected[ARTIST_COL]} | {selected[GENRE_COL]} | {selected[EMOTION_COL]} | "
        f"{int(selected['release_year']) if pd.notna(selected['release_year']) else 'Unknown year'}"
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Dự đoán", predicted_label)
    c2.metric("Tích cực", f"{positive_percent:.1f}%")
    c3.metric("Tiêu cực", f"{negative_percent:.1f}%")

    st.progress(int(round(positive_percent)), text=f"Tích cực: {positive_percent:.1f}%")
    st.progress(int(round(negative_percent)), text=f"Tiêu cực: {negative_percent:.1f}%")

    prediction_df = pd.DataFrame(
        {
            "Sentiment": ["Positive", "Negative"],
            "Percent": [positive_percent, negative_percent],
        }
    )
    require_plotly()
    fig = px.bar(
        prediction_df,
        x="Sentiment",
        y="Percent",
        color="Sentiment",
        text="Percent",
        title="Tỉ lệ dự đoán cảm xúc của bài hát",
        range_y=[0, 100],
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig.update_layout(height=420, margin=dict(l=10, r=10, t=55, b=10), xaxis_title="", yaxis_title="Phần trăm")
    st.plotly_chart(fig, use_container_width=True)

    detail = pd.DataFrame(
        [
            {
                "Artist": selected[ARTIST_COL],
                "Song": selected[SONG_COL],
                "Genre": selected[GENRE_COL],
                "Emotion": selected[EMOTION_COL],
                "Release Year": selected["release_year"],
                "Popularity": selected["Popularity_num"],
                "Energy": selected["Energy_num"],
                "Danceability": selected["Danceability_num"],
                "Positiveness": selected["Positiveness_num"],
            }
        ]
    )
    st.dataframe(detail, use_container_width=True, hide_index=True)


def data_tab(df):
    st.caption("Bảng chỉ lấy tối đa số dòng đang chọn để dashboard phản hồi nhanh.")
    row_limit = st.slider("Số dòng hiển thị / tải xuống", 100, 5000, 1000, step=100)
    sort_choice = st.selectbox(
        "Sắp xếp",
        ["Popularity cao nhất", "Positiveness cao nhất", "Energy cao nhất", "Danceability cao nhất", "Mới nhất"],
    )

    sort_map = {
        "Popularity cao nhất": F.desc("Popularity_num"),
        "Positiveness cao nhất": F.desc("Positiveness_num"),
        "Energy cao nhất": F.desc("Energy_num"),
        "Danceability cao nhất": F.desc("Danceability_num"),
        "Mới nhất": F.desc("release_year"),
    }

    table = to_pandas(df.select(*DISPLAY_COLUMNS).orderBy(sort_map[sort_choice]), row_limit)
    st.dataframe(table, use_container_width=True, hide_index=True)

    st.download_button(
        "Tải bảng đang xem CSV",
        data=table.to_csv(index=False).encode("utf-8"),
        file_name="spotify_dashboard_filtered.csv",
        mime="text/csv",
    )


def main() -> None:
    require_plotly()

    st.title("Spotify Sentiment Dashboard")
    st.markdown(
        '<p class="section-note">Theo dõi dữ liệu đã clean từ parquet, lọc nhanh theo sentiment, popularity, genre, emotion, artist và bài hát.</p>',
        unsafe_allow_html=True,
    )

    if not DATA_PATH.exists():
        st.error(f"Không tìm thấy dữ liệu: {DATA_PATH}")
        st.stop()

    df = load_prepared_data()
    filters = build_sidebar(df)
    filtered = apply_filters(df, filters)

    draw_metric_row(filtered)

    tab_overview, tab_sentiment, tab_audio, tab_prediction, tab_data = st.tabs(
        ["Tổng quan", "Cảm xúc", "Audio features", "Dự đoán bài hát", "Bảng dữ liệu"]
    )
    with tab_overview:
        overview_tab(filtered)
    with tab_sentiment:
        sentiment_tab(filtered)
    with tab_audio:
        audio_tab(filtered)
    with tab_prediction:
        prediction_tab(filtered)
    with tab_data:
        data_tab(filtered)


if __name__ == "__main__":
    main()
