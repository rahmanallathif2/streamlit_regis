import psycopg2
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from io import BytesIO

# Fungsi untuk mengimpor file Excel atau CSV
def upload_file(uploaded_file):
    if uploaded_file.name.endswith('.csv'):
        return pd.read_csv(uploaded_file)
    elif uploaded_file.name.endswith('.xlsx'):
        return pd.read_excel(uploaded_file)
    else:
        raise ValueError("Format file tidak didukung. Silakan upload file CSV atau Excel.")

# Fungsi untuk memproses data menggunakan query SQL
def process_data(dataregis_df, masterkel_df):
    conn = psycopg2.connect(
        dbname="postgres",  # Nama database PostgreSQL Anda
        user="postgres",    # Nama user PostgreSQL Anda
        password="Admin",   # Password PostgreSQL Anda
        host="localhost",   # Host database Anda
        port="5432"         # Port database Anda
    )
    cursor = conn.cursor()

    # Membersihkan tabel sebelum menyisipkan data baru
    cursor.execute("DELETE FROM dataregis;")
    cursor.execute("DELETE FROM masterkel;")

    # Menyisipkan data ke dalam PostgreSQL (manual insert)
    for index, row in dataregis_df.iterrows():
        cursor.execute("INSERT INTO dataregis (no_polisi, full_address, kd_camat, kecamatan, nm_merek_kb, nm_model_kb, kd_jenis_kb, jenis_kendaraan, th_buatan, no_chasis, no_mesin, warna_kb, tg_pros_bayar) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
                       (row['no_polisi'], row['full_address'], row['kd_camat'], row['kecamatan'], row['nm_merek_kb'], row['nm_model_kb'], row['kd_jenis_kb'], row['jenis_kendaraan'], row['th_buatan'], row['no_chasis'], row['no_mesin'], row['warna_kb'], row['tg_pros_bayar']))

    for index, row in masterkel_df.iterrows():
        cursor.execute("INSERT INTO masterkel (kelurahan, kecamatan, kelurahan_master, kecamatan_master) VALUES (%s, %s, %s, %s)", 
                       (row['kelurahan'], row['kecamatan'], row['kelurahan_master'], row['kecamatan_master']))

    conn.commit()

    query = """
    WITH MatchedData AS (
        SELECT 
            dr.no_polisi,
            dr.full_address,
            dr.kd_camat,
            dr.kecamatan,
            dr.nm_merek_kb,
            dr.nm_model_kb,
            dr.kd_jenis_kb,
            dr.jenis_kendaraan,
            dr.th_buatan,
            dr.no_chasis,
            dr.no_mesin,
            dr.warna_kb,
            dr.tg_pros_bayar,
            mk.kelurahan AS kelurahan_masterkel,
            mk.kecamatan AS kecamatan_masterkel,
            mk.kelurahan_master AS kelurahan_master,
            mk.kecamatan_master AS kecamatan_master,
            ROW_NUMBER() OVER (PARTITION BY dr.no_polisi ORDER BY dr.no_polisi) AS rn
        FROM dataregis dr
        LEFT JOIN masterkel mk 
          ON dr.full_address LIKE CONCAT('%', mk.kelurahan, '%')
    )
    SELECT *
    FROM MatchedData
    WHERE rn = 1;
    """
    result_df = pd.read_sql(query, conn)
    conn.close()
    return result_df

# Fungsi untuk menyimpan dataframe ke Excel dan membuat link unduh
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Hasil Proses')
    processed_data = output.getvalue()
    return processed_data

# Fungsi utama untuk menjalankan aplikasi Streamlit
def main():
    st.title("Aplikasi Pengolahan Data")

    # Upload file dataregis dan master kelurahan
    dataregis_file = st.file_uploader("Upload file dataregis (CSV atau Excel)", type=["csv", "xlsx"])
    masterkel_file = st.file_uploader("Upload file masterkel (CSV atau Excel)", type=["csv", "xlsx"])

    if dataregis_file is not None and masterkel_file is not None:
        try:
            dataregis_df = upload_file(dataregis_file)
            masterkel_df = upload_file(masterkel_file)

            st.write("Kolom pada file dataregis:", dataregis_df.columns)
            st.write("Kolom pada file masterkel:", masterkel_df.columns)

            result_df = process_data(dataregis_df, masterkel_df)

            st.write("Hasil Proses Data:")
            st.write(result_df)

            # Tombol untuk mengunduh hasil dalam format Excel
            excel_data = to_excel(result_df)
            st.download_button(
                label="Unduh Hasil dalam Format Excel",
                data=excel_data,
                file_name="hasil_proses_data.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        except Exception as e:
            st.error(f"Terjadi kesalahan: {e}")

# Jalankan aplikasi Streamlit
if __name__ == "__main__":
    main()
