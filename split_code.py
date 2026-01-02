#!/usr/bin/env python3
"""
Скрипт для разделения moviebot.py на модули
"""
import re
import os

def extract_function(code, func_name):
    """Извлекает функцию из кода"""
    pattern = rf'^def {func_name}\(.*?)(?=^def |\Z)'
    match = re.search(pattern, code, re.MULTILINE | re.DOTALL)
    if match:
        return match.group(0)
    return None

def extract_functions_by_keywords(code, keywords):
    """Извлекает функции, содержащие ключевые слова"""
    functions = {}
    lines = code.split('\n')
    current_func = None
    current_code = []
    in_func = False
    indent = 0
    
    for line in lines:
        # Начало функции
        match = re.match(r'^def (\w+)\(', line)
        if match:
            # Сохраняем предыдущую функцию если она подходит
            if current_func and any(kw in '\n'.join(current_code) for kw in keywords):
                functions[current_func] = '\n'.join(current_code)
            current_func = match.group(1)
            current_code = [line]
            in_func = True
            indent = len(line) - len(line.lstrip())
        elif in_func:
            # Проверяем, не закончилась ли функция
            if line.strip() and not line.startswith(' ') and not line.startswith('\t'):
                if not line.startswith('@') and not line.startswith('#'):
                    # Сохраняем функцию
                    if current_func and any(kw in '\n'.join(current_code) for kw in keywords):
                        functions[current_func] = '\n'.join(current_code)
                    current_func = None
                    current_code = []
                    in_func = False
            else:
                current_code.append(line)
    
    # Сохраняем последнюю функцию
    if current_func and any(kw in '\n'.join(current_code) for kw in keywords):
        functions[current_func] = '\n'.join(current_code)
    
    return functions

# Читаем moviebot.py
with open('moviebot.py', 'r', encoding='utf-8') as f:
    content = f.read()

# API функции Kinopoisk
api_keywords = ['KP_TOKEN', 'kinopoiskapiunofficial.tech', 'extract_movie_info']
api_functions = extract_functions_by_keywords(content, api_keywords)

print(f"Найдено {len(api_functions)} API функций:")
for name in sorted(api_functions.keys()):
    print(f"  - {name}")

# Сохраняем в файл для проверки
with open('api_functions_list.txt', 'w', encoding='utf-8') as f:
    for name in sorted(api_functions.keys()):
        f.write(f"{name}\n")

print("\nСписок сохранен в api_functions_list.txt")


