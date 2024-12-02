import csv

def exportar_grade(file_path):
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        students = []
        
        for row in reader:
            name = row[0]
            grades = sorted(map(int, row[1:]), reverse=True)[:3]
            average = round(sum(grades) / 3, 2)
            students.append((name, grades, average))
        
        students_sorted = sorted(students, key=lambda x: x[0])
        
        for student in students_sorted:
            print(f"Nome: {student[0]} Notas: {student[1]} Média: {student[2]}")

# Exemplo de chamada da função
exportar_grade('/c:/Users/Usuario/Desktop/compassacademy/Sprint-4/Exercicios/estudantes.csv')
