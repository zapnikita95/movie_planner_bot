import asyncio
import json
import re
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest

# Твои данные
api_id = 21084933
api_hash = '4c9d05d055999a6ea7e06d25028e18a0'

# Username группы без @
group_username = 'homemoviegroup'

# УЛУЧШЕННАЯ регулярка: захватывает ссылки с параметрами (?...)
kp_pattern = re.compile(r'(https?://(?:www\.)?kinopoisk\.ru/(film|series)/\d+/?[\w/-]*\??[^"\s<>\)\]]*?)')

async def main():
    client = TelegramClient('kp_export_session', api_id, api_hash)
    
    print("Подключаюсь к Telegram...")
    await client.start()
    
    print(f"Доступ к группе @{group_username}...")
    try:
        entity = await client.get_entity(group_username)
    except Exception as e:
        print(f"Ошибка: {e}")
        print("Проверь username группы или доступ.")
        return

    links = []
    offset_id = 0
    limit = 100
    total_found = 0

    print("Скачиваю сообщения и ищу ссылки на Кинопоиск (включая с параметрами)...\n")

    # Тест регулярки
    test = "https://www.kinopoisk.ru/film/5312505/?utm_source=share"
    if kp_pattern.search(test):
        print("✓ Регулярка работает с параметрами!")
    else:
        print("✗ Регулярка сломана — сообщи мне.")

    while True:
        history = await client(GetHistoryRequest(
            peer=entity,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0
        ))

        if not history.messages:
            break

        for message in history.messages:
            text = getattr(message, 'message', '') or ''
            
            # Добавляем URL из entities (если есть)
            if message.entities:
                for entity in message.entities:
                    if hasattr(entity, 'url'):
                        text += ' ' + entity.url  # TextUrl
                    else:
                        # Url entity — извлекаем по offset/length
                        start = entity.offset
                        end = entity.offset + entity.length
                        if start < len(text) and end <= len(text):
                            text += ' ' + text[start:end]

            # Ищем все совпадения
            for match in kp_pattern.finditer(text):
                raw_link = match.group(1)
                # Очищаем от параметров и www
                clean_link = re.sub(r'\?.*$', '', raw_link)  # Убираем ?...
                clean_link = re.sub(r'/+$', '', clean_link)
                clean_link = re.sub(r'^https?://www\.', 'https://', clean_link)
                
                links.append({
                    'original_link': raw_link,
                    'clean_link': clean_link,
                    'date': message.date.isoformat(),
                    'message_id': message.id
                })
                total_found += 1

        offset_id = history.messages[-1].id
        print(f"Обработано ~{offset_id} сообщений, найдено ссылок: {total_found}", end='\r')

        if len(history.messages) < limit:
            break

    # Убираем дубли по clean_link
    unique_links = {item['clean_link']: item for item in links}
    unique_list = list(unique_links.values())

    with open('kinopoisk_links.json', 'w', encoding='utf-8') as f:
        json.dump(unique_list, f, ensure_ascii=False, indent=4)

    print(f"\n\nГотово!")
    print(f"Найдено упоминаний: {total_found}")
    print(f"Уникальных фильмов/сериалов: {len(unique_list)}")
    
    if unique_list:
        print("\nПримеры найденных ссылок:")
        for i, item in enumerate(unique_list[:10], 1):
            print(f"  {i}. {item['clean_link']} (оригинал: {item['original_link']})")
    else:
        print("\n⚠️ Ссылок не найдено. Возможные причины:")
        print("  - Ссылки в группе в другом формате (например, короткие t.me или без http)")
        print("  - Группа большая — попробуй запустить заново")

    print("\nФайл: kinopoisk_links.json")

    await client.disconnect()

asyncio.run(main())
