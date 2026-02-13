import os

print("СТАРТ СКРИПТА")
print("Файлы в папке:", os.listdir())

with open("catalog.txt", "r", encoding="utf-8") as f:
    data = f.read()

print("Содержимое catalog.txt:")
print(data)

print("СКРИПТ ДОШЕЛ ДО КОНЦА")
