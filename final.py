import mysql.connector
import threading
from tqdm import tqdm

thread_locks = {}
thread_bars = {}
thread_sums = {}
thread_ids = {}  


def format_name(name):
    """Formato para el nombre: rellena con guiones bajos a la izquierda."""
    return (name[-12:]).rjust(12, '_')

def fetch_and_sum_children(thread_id, start_id, end_id):
    """Fetch and sum 'children' values for individual queries from a specific range of IDs."""
    connection = mysql.connector.connect(
        host="localhost",
        port="3307",
        user="root",
        password="41818",
        database="parallel_sum"
    )
    
    cursor = connection.cursor()

    total_sum = 0
    ids_sumados = []

    for record_id in range(start_id, end_id):
            query = f"SELECT children, name, lastname FROM personas WHERE id = {record_id}"
            cursor.execute(query)
            row = cursor.fetchone()
            if row:  
                total_sum += row[0]
                ids_sumados.append(record_id)  

                name = format_name(row[1])  
                lastname = format_name(row[2])  

                with thread_locks[thread_id]:
                    thread_sums[thread_id] = total_sum  
                    thread_bars[thread_id].set_postfix({
                        "Current Sum": total_sum,
                        "Id Procesado": record_id,
                        "Nombre": name,  
                        "Apellido": lastname  
                    })
                    thread_bars[thread_id].update(1)  # Actualiza el Progreso de la Barraa en 1

    cursor.close()
    connection.close()

    # Devolver la suma total y los IDs sumados
    return total_sum, ids_sumados

def main():
    connection = mysql.connector.connect(
        host="localhost",
        port="3307",
        user="root",
        password="41818",
        database="parallel_sum"
    )
    
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM personas")
    total_records = cursor.fetchone()[0]
    
    cursor.close()
    connection.close()

    batch_size = 100000  
    total_batches = total_records // batch_size + (1 if total_records % batch_size != 0 else 0)

    total_children = 0
    
    max_threads = 10
    for i in range(max_threads):  
        thread_locks[i] = threading.Lock()
        thread_bars[i] = tqdm(total=batch_size, desc=f"Thread {i+1}", position=i, leave=True)
        thread_sums[i] = 0  
        thread_ids[i] = []  

    threads = []  
    for i in range(total_batches):
        start_id = i * batch_size + 1
        end_id = min(start_id + batch_size, total_records + 1)  
        thread_id = i % max_threads
        
        thread = threading.Thread(target=lambda tid=thread_id, sid=start_id, eid=end_id: thread_ids[tid].extend(fetch_and_sum_children(tid, sid, eid)))
        threads.append(thread)  
        thread.start()  

    for thread in threads:
        thread.join()

    for bar in thread_bars.values():
        bar.close()

    total_children = sum(thread_sums.values())


    print(f"Suma total de la columna children: {total_children}")

if __name__ == "__main__":
    main()

