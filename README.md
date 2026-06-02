# Big Data Sentiment Analysis on Spotify Songs

Du an nay phan tich cam xuc bai hat Spotify bang PySpark va Spark MLlib. Notebook chinh la `btl_big_data.ipynb`, su dung tap du lieu `spotify_dataset.csv`, lam sach du lieu, tao nhan `sentiment_label` tu cot `Positiveness`, truc quan hoa dac trung am thanh va huan luyen cac mo hinh phan loai cam xuc.

## Xem nhanh tren GitHub

Neu GitHub khong render duoc file notebook `.ipynb`, bam vao ban Markdown de xem truc tiep:

[Xem bao cao notebook](btl_big_data.md)

## 1. Cau truc thu muc

```text
.
├── btl_big_data.ipynb              # Notebook bai tap lon
├── btl_big_data.md                 # Ban xem nhanh tren GitHub
├── btl_big_data_files/             # Anh output cho ban Markdown
├── spotify_dataset.csv             # Du lieu goc Spotify
├── spotify_data_clean.parquet/     # Du lieu da luu dang Parquet
├── spark-3.5.8-bin-hadoop3/        # Apache Spark local
├── spark-3.5.8-bin-hadoop3.tgz     # File nen Spark
├── myenv/                          # Moi truong Python co san
├── README.md                       # Huong dan chay
└── slide.txt                       # Goi y thiet ke slide bao cao
```

## 2. Yeu cau moi truong

- Ubuntu/Linux hoac moi truong co terminal Bash.
- Python 3.8 tro len.
- Java JDK da cai dat. Co the kiem tra bang:

```bash
java -version
```

- Apache Spark 3.5.8. Thu muc Spark da co san trong repo: `spark-3.5.8-bin-hadoop3`.
- Cac thu vien Python can dung: `findspark`, `pyspark`, `pandas`, `matplotlib`, `seaborn`, `scikit-learn`, `jupyter`.

## 3. Thiet lap moi truong

Tu thu muc goc du an:

```bash
cd /home/phongtre/Desktop/Humungousour
```

Neu muon dung moi truong ao da co san:

```bash
source myenv/bin/activate
```

Thiet lap bien moi truong Spark:

```bash
export SPARK_HOME=/home/phongtre/Desktop/Humungousour/spark-3.5.8-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_PYTHON=python3
```

Neu thieu thu vien Python, cai dat bo thu vien can thiet:

```bash
pip install findspark pyspark pandas matplotlib seaborn scikit-learn jupyter
```

## 4. Cach chay notebook

Mo Jupyter Notebook:

```bash
jupyter notebook
```

Sau do mo file:

```text
btl_big_data.ipynb
```

Chay lan luot cac cell tu tren xuong duoi. Khong nen chay rieng le cac cell mo hinh khi chua chay cac cell chuan bi du lieu, vi cac bien nhu `spark`, `df`, `train_final`, `test_final` can duoc tao truoc.

## 4.1. Cach chay dashboard local

Dashboard Streamlit doc truc tiep du lieu `spotify_data_clean.parquet` bang Spark va ho tro loc nhanh theo sentiment, popularity, genre, emotion, artist va ten bai hat.

Tu thu muc goc du an:

```bash
cd /home/phongtre/Desktop/Humungousour
source myenv/bin/activate
pip install -r requirements-dashboard.txt
streamlit run dashboard.py
```

Sau khi chay, mo duong dan Streamlit hien tren terminal, thuong la:

```text
http://localhost:8501
```

Neu Spark khong khoi dong duoc, thiet lap lai bien moi truong:

```bash
export SPARK_HOME=/home/phongtre/Desktop/Humungousour/spark-3.5.8-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_PYTHON=/home/phongtre/Desktop/Humungousour/myenv/bin/python
export SPARK_LOCAL_IP=127.0.0.1
```

## 5. Quy trinh xu ly trong notebook

1. Khoi tao Spark Session voi che do local `local[*]`.
2. Doc file `spotify_dataset.csv` bang Spark.
3. Kiem tra gia tri null tren toan bo dataset.
4. Xoa cac dong co gia tri null.
5. Tao cot `sentiment_label`:
   - `Positiveness >= 50`: tich cuc, nhan `1.0`.
   - `Positiveness < 50`: tieu cuc, nhan `0.0`.
6. Phan tich tuong quan giua cac dac trung am thanh va nhan cam xuc.
7. Truc quan hoa bang heatmap, box plot, pie chart, bar chart.
8. Chuan bi du lieu cho Spark MLlib:
   - Lam sach cot `Loudness (db)` thanh `Loudness_clean`.
   - Ep kieu cac cot dac trung ve `DoubleType`.
   - Tao them feature engineering: `Energy_x_Danceability`, `Acoustic_x_Instrumental`, `Speech_x_Liveness`.
   - Gom dac trung bang `VectorAssembler`.
   - Chuan hoa bang `StandardScaler`.
   - Chia train/test theo ty le 80/20.
9. Huan luyen va danh gia 3 mo hinh:
   - Logistic Regression.
   - Decision Tree.
   - Random Forest.

## 6. Ket qua tham khao

Ket qua da luu trong notebook:

| Noi dung | Gia tri |
| --- | ---: |
| So dong ban dau | 551,443 |
| So cot | 39 |
| Tong gia tri null | 100 |
| So dong sau lam sach | 551,385 |
| Nhan tieu cuc | 324,442 bai, 58.8% |
| Nhan tich cuc | 226,943 bai, 41.2% |
| Train sau khi fix AUC | 368,580 dong |
| Test sau khi fix AUC | 92,045 dong |

Ket qua mo hinh:

| Mo hinh | AUC-ROC | Recall | Accuracy |
| --- | ---: | ---: | ---: |
| Logistic Regression | 0.7613 | 0.6236 | 0.69 |
| Decision Tree | 0.7904 | 0.6741 | 0.72 |
| Random Forest | 0.7909 | 0.6894 | 0.72 |

Nhan xet ngan: Random Forest cho AUC-ROC cao nhat va recall tot nhat trong 3 mo hinh, nen co the chon lam mo hinh de bao cao ket qua cuoi.

## 7. Loi thuong gap

### Khong tim thay Spark

Kiem tra lai `SPARK_HOME`:

```bash
echo $SPARK_HOME
```

Neu bien moi truong rong, chay lai:

```bash
export SPARK_HOME=/home/phongtre/Desktop/Humungousour/spark-3.5.8-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH
```

### Loi Java

Neu Spark bao loi lien quan Java, kiem tra:

```bash
java -version
```

Neu chua co Java, can cai JDK truoc khi chay Spark.

### Notebook chay cham hoac het RAM

Dataset CSV gan 1.1 GB, nen cac lenh `count()`, `toPandas()` va ve bieu do co the ton RAM. Nen dong cac ung dung khac, chay theo thu tu tung cell, va khong restart kernel giua chung.

### Loi bien chua duoc dinh nghia

Hay chay lai notebook tu dau theo thu tu. Rieng cac cell Logistic Regression, Decision Tree va Random Forest can co `train_final` va `test_final`, duoc tao o cell "Chuan bi lai dataset cho mo hinh - fix AUC".
