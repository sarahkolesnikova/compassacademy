import csv

def exportar_grades(file_path):
    with open(file_path, newline='') as csvfile:
        arquivo = csv.reader(csvfile)
        estudantes = []
        
        for row in arquivo:
            nome = row[0]
            grades = sorted(map(int, row[1:]), reverse=True)[:3]
            media = round(sum(grades) / 3, 2)
            estudantes.append((nome, grades, media))
        
        estudantes_sorted = sorted(estudantes, key=lambda x: x[0])
        
        for estudante in estudantes_sorted:
            print(f"Nome: { estudante[0]} Notas: { estudante[1]} Média: { estudante[2]}")
exportar_grades ("estudantes.csv")
