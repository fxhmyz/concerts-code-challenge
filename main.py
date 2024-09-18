import psycopg2

def get_band_for_concert(concert_id):
    conn = psycopg2.connect(
        dbname="concerts_py", user="ali", password="wambua", host="localhost"
    )
    cur = conn.cursor()
    cur.execute('''
        SELECT bands.* FROM concerts
        JOIN bands ON concerts.band_id = bands.id
        WHERE concerts.id = %s
    ''', (concert_id,))
    band = cur.fetchone()
    conn.close()
    
    print(f"Band for concert {concert_id}: {band}")
    return band

def get_concerts_for_venue(venue_id):
    conn = psycopg2.connect(
        dbname="concerts_py", user="ali", password="wambua", host="localhost"
    )
    cur = conn.cursor()
    cur.execute('''
        SELECT concerts.* FROM concerts
        WHERE concerts.venue_id = %s
    ''', (venue_id,))
    concerts = cur.fetchall()
    conn.close()
    
    print(f"Concerts for venue {venue_id}: {concerts}")
    return concerts

def play_in_venue(band_id, venue_id, date):
    conn = psycopg2.connect(
        dbname="concerts_py", user="ali", password="wambua", host="localhost"
    )
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO concerts (band_id, venue_id, date)
        VALUES (%s, %s, %s)
    ''', (band_id, venue_id, date))
    conn.commit()
    conn.close()
    
    print(f"Band {band_id} played at venue {venue_id} on {date}")

def is_hometown_show(concert_id):
    conn = psycopg2.connect(
        dbname="concerts_py", user="ali", password="wambua", host="localhost"
    )
    cur = conn.cursor()
    cur.execute('''
        SELECT bands.hometown, venues.city FROM concerts
        JOIN bands ON concerts.band_id = bands.id
        JOIN venues ON concerts.venue_id = venues.id
        WHERE concerts.id = %s
    ''', (concert_id,))
    result = cur.fetchone()
    conn.close()
    
    is_hometown = result[0] == result[1]  # compares band hometown to venue city
    print(f"Concert {concert_id} is a hometown show: {is_hometown}")
    return is_hometown

def get_band_with_most_performances():
    conn = psycopg2.connect(
        dbname="concerts_py", user="ali", password="wambua", host="localhost"
    )
    cur = conn.cursor()
    cur.execute('''
        SELECT bands.name, COUNT(concerts.id) as concert_count
        FROM bands
        JOIN concerts ON concerts.band_id = bands.id
        GROUP BY bands.id
        ORDER BY concert_count DESC
        LIMIT 1
    ''')
    band = cur.fetchone()
    conn.close()
    
    print(f"Band with the most performances: {band}")
    return band

def main():
    # Example function calls
    print(get_band_for_concert(1))
    print(get_concerts_for_venue(1))
    play_in_venue(1, 2, '2024-10-01')
    print(is_hometown_show(1))
    print(get_band_with_most_performances())

if __name__ == "__main__":
    main()
