"""Taller evaluable de Word Count"""

# pylint: disable=broad-exception-raised

import glob
import os
import os.path
import string
import time

def clear_folder(folder):
    """Elimina los archivos dentro de una carpeta."""
    if os.path.exists(folder):
        for file in glob.glob(os.path.join(folder, "*")):
            os.remove(file)

def initialize_folder(folder):
    """Crea la carpeta si no existe, o la limpia si ya existe."""
    if os.path.exists(folder):
        clear_folder(folder)
    else:
        os.makedirs(folder)

def delete_folder(folder):
    """Elimina la carpeta y su contenido."""
    if os.path.exists(folder):
        clear_folder(folder)
        os.rmdir(folder)

def generate_file_copies(n):
    """Genera n copias de los archivos en files/raw dentro de files/input."""
    initialize_folder("files/input/")
    
    raw_files = glob.glob("files/raw/*")
    for file_path in raw_files:
        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read()

        raw_filename = os.path.basename(file_path)
        name, ext = os.path.splitext(raw_filename)
        
        for i in range(1, n + 1):
            new_filename = f"{name}_{i}{ext}"
            with open(f"files/input/{new_filename}", "w", encoding="utf-8") as f2:
                f2.write(text)

def mapper(sequence):
    """Mapea las líneas a pares (palabra, 1)."""
    pairs_sequence = []
    for _, line in sequence:
        line = line.lower()
        # Elimina puntuación
        line = line.translate(str.maketrans("", "", string.punctuation))
        words = line.split()
        pairs_sequence.extend([(word, 1) for word in words])
    return pairs_sequence

def reducer(pairs_sequence):
    """Suma los valores por cada palabra (la secuencia debe estar ordenada)."""
    result = []
    for key, value in pairs_sequence:
        if result and result[-1][0] == key:
            result[-1] = (key, result[-1][1] + value)
        else:
            result.append((key, value))
    return result

def hadoop(input_folder, output_folder, mapper_fn, reducer_fn):
    """Simula el flujo de Hadoop MapReduce."""
    
    def read_records_from_input(folder):
        sequence = []
        files = glob.glob(os.path.join(folder, "*"))
        for file in files:
            with open(file, "r", encoding="utf-8") as f:
                for line in f:
                    sequence.append((file, line))
        return sequence

    def save_results_to_output(res, out_folder):
        with open(os.path.join(out_folder, "part-00000"), "w", encoding="utf-8") as f:
            for key, value in res:
                f.write(f"{key}\t{value}\n")

    def create_success_file(out_folder):
        with open(os.path.join(out_folder, "_SUCCESS"), "w", encoding="utf-8") as f:
            f.write("")

    # Ejecución del flujo
    if os.path.exists(output_folder):
        delete_folder(output_folder)
    os.makedirs(output_folder)

    sequence = read_records_from_input(input_folder)
    pairs = mapper_fn(sequence)
    pairs.sort() # El shuffle/sort es crucial antes del reducer
    reduced_result = reducer_fn(pairs)
    
    save_results_to_output(reduced_result, output_folder)
    create_success_file(output_folder)

if __name__ == "__main__":
    # Prueba local
    generate_file_copies(10) # n=10 para una prueba rápida
    start_time = time.time()

    hadoop(
        input_folder="files/input/",
        output_folder="files/output/",
        mapper_fn=mapper,
        reducer_fn=reducer,
    )

    print(f"Tiempo de ejecución: {time.time() - start_time:.2f} segundos")