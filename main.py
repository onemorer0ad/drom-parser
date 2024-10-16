import random
import asyncio
import aiohttp
import pandas as pd
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse
import random

auto_urls_lst = []
cars_data = []

with open('proxies.txt', 'r') as f:
    PROXY_LIST = [proxy for proxy in f.read().splitlines() if proxy]

# Max retries for a proxy
MAX_RETRIES = 3

async def send_request(session, url, random_proxy, retry_count=0):
    try:
        async with session.get(url, proxy=random_proxy, headers={'User-Agent': UserAgent().random}) as response:
            if response.status == 200:
                print('Запрос успешно отправлен')
                return await response.text()
            else:
                print(f"Error: {response.status} for {url}")
                return None
    except Exception as e:
        print(f"Error with proxy {random_proxy}: {e}")
        if retry_count < MAX_RETRIES:
            # Retry with a different proxy
            new_proxy = random.choice(PROXY_LIST)
            print(f"Retrying with new proxy: {new_proxy}")
            return await send_request(session, url, new_proxy, retry_count + 1)
        else:
            print(f"Max retries reached for {url} with proxy {random_proxy}")
            return None

async def parse_auto_urls(session, category_url, random_proxy):
    try:
        html_response = await send_request(session, category_url, random_proxy)
        if html_response:
            soup = BeautifulSoup(html_response, 'lxml')
            car_items = soup.find_all('div', class_='css-1f68fiz ea1vuk60')  # Класс для контейнера объявлений
            print(f"Найдено {len(car_items)} объявлений на странице: {category_url}")
            
            for car_item in car_items:
                try:
                    exclude_class = car_item.find('h3', class_='css-d4igzo efwtv890')
                    if exclude_class:
                        continue


                    link_url = car_item.find('a', class_='g6gv8w4 g6gv8w8 _1ioeqy90').get('href')
                    print(link_url)
                    # Проверка ссылки на принадлежность к нужному городу
                    domain = f"{category_url.split('//')[1].split('.')[0]}"
                    if domain in link_url or link_url.startswith('/'):
                        full_url = f"https://{domain}.drom.ru{link_url}" if link_url.startswith('/') else link_url
                        print('Добавил в список', full_url)
                        auto_urls_lst.append(full_url)

                    else:
                        None
                except AttributeError as e:
                    print(f"Ошибка при парсинге ссылки на объявление: {e}")
        else:
            print(f"Ошибка: Нет HTML ответа для {category_url}")
    except Exception as e:
        print(f"Ошибка парсинга URL {category_url}: {e}")

    return auto_urls_lst


async def parse_car_data(session, auto_url, random_proxy):
    html_response = await send_request(session, auto_url, random_proxy)

    if html_response:
        soup = BeautifulSoup(html_response, 'lxml')
        data = soup.find('div', class_='css-0 epjhnwz1')

        # Initialize the data dictionary with default values
        car_data_dict = {
                'Название': 'N/A',
                'Цена': 'N/A',
                'Двигатель': 'N/A',
                'Мощность': 'N/A',
                'Коробка': 'N/A',
                'Привод': 'N/A',
                'Тип кузова': 'N/A',
                'Цвет': 'N/A',
                'Пробег': 'N/A',
                'Руль': 'N/A',
                'Птс': 'N/A',
                'Регистрация': 'N/A',
                'Юридическое лицо': 'N/A',
                'Розыск': 'N/A',
            }

        if data:
               # Extracting title
            title_data = soup.findAll('span', class_='css-1kb7l9z e162wx9x0')
            print(title_data[0].text)
            if title_data:
                car_data_dict['Название'] = title_data[0].text

            # Extracting price
            if data:
                price_div = data.find('div', class_='wb9m8q0')
                if price_div:
                    car_data_dict['Цена'] = price_div.text.replace('₽', '').replace('\u00A0', '')

            # Extracting car data
            car_data = data.findAll('td', class_='css-1la7f7n ezjvm5n0')
            labels = ['Двигатель', 'Мощность', 'Коробка', 'Привод', 'Цвет', 'Пробег', 'Руль', 'Тип кузова']
            for i, label in enumerate(labels):
                car_data_dict[label] = car_data[i].text

            # Extracting VIN data
            vin_data_button = data.findAll('button', class_='g6gv8w4 g6gv8w7 g6gv8w6')
            if len(vin_data_button) > 0:
                car_data_dict['Птс'] = vin_data_button[0].text
            if len(vin_data_button) > 1:
                car_data_dict['Регистрация'] = vin_data_button[1].text

            vin_data_text = data.findAll('div', class_='css-13qo6o5 e1mhp2ux0')
            if len(vin_data_text) > 1:
                car_data_dict['Юридическое лицо'] = vin_data_text[1].text
            if len(vin_data_text) > 3:
                car_data_dict['Розыск'] = vin_data_text[3].text

            # Append data to cars_data list
        car_data_dict['Ссылка'] = auto_url
        cars_data.append(car_data_dict)

async def count_pages(session, url):
    html_response = await send_request(session, url, random.choice(PROXY_LIST))
    if html_response:
        soup = BeautifulSoup(html_response, 'lxml')
        count_announcements = soup.find('div', class_='css-1xkq48l eckkbc90').text.split()[0]
        if count_announcements:
            total_pages = (int(count_announcements) // 20) + (1 if int(count_announcements) % 20 != 0 else 0 )
        return total_pages
    return 0

def update_url_with_page(url, page_number):
    # Разбиваем URL на части: схема, домен, путь, параметры и т.д.
    parsed_url = urlparse(url)

    # Если это не первая страница, добавляем номер страницы в конец пути
    if page_number > 1:
        # Убираем слеш в конце пути, если есть, и добавляем /page{номер страницы}/
        path = parsed_url.path.rstrip('/') + f'/page{page_number}/'
    else:
        # Для первой страницы путь оставляем как есть
        path = parsed_url.path

    # Собираем обновленный URL обратно
    updated_url = urlunparse((
        parsed_url.scheme,   # Схема (например, https)
        parsed_url.netloc,   # Домен (например, moscow.drom.ru)
        path,                # Обновленный путь
        parsed_url.params,   # Параметры (если есть)
        parsed_url.query,    # Строка запроса (например, minprice=500000&maxprice=1000000)
        parsed_url.fragment  # Фрагмент (обычно пуст)
    ))

    # Возвращаем обновленный URL
    return updated_url

async def process_url(session, url, proxy):
    # Обработка первой страницы
    print(f"Processing base URL: {url}")
    await parse_auto_urls(session, url, proxy)
    await asyncio.sleep(random.randint(2, 3))  # задержка между запросами

    # Получаем количество страниц
    total_pages = await count_pages(session, url)
    print(f"Total Pages: {total_pages}")

    # Обрабатываем остальные страницы начиная со второй
    for page_number in range(2, total_pages + 1):
        page_url = update_url_with_page(url, page_number)
        print(f"Processing page URL: {page_url}")
        await parse_auto_urls(session, page_url, proxy)
        await asyncio.sleep(random.randint(2, 3))  # задержка между запросами

async def main():
    # считываем файл с ссылками

    with open('urls_list.txt', 'r') as file:
        urls_list = [url.strip() for url in file if url.strip()]
        print(urls_list)
    # Использование ClientSession для запросов
    async with aiohttp.ClientSession() as session:
        tasks = []

        for url in urls_list:
            # Обработка первой страницы отдельно
            random_proxy = random.choice(PROXY_LIST)
            tasks.append(process_url(session, url, random_proxy))
            await asyncio.sleep(random.randint(2, 3))  # задержка между запросами

        # Запуск всех задач для других страниц
        await asyncio.gather(*tasks)

        # Парсинг данных авто по всем собранным ссылкам
        car_tasks = []
        print(f"Total cars found: {len(auto_urls_lst)}")
        for auto_url in auto_urls_lst:
            random_proxy = random.choice(PROXY_LIST)
            car_tasks.append(parse_car_data(session, auto_url, random_proxy))
            await asyncio.sleep(random.randint(2, 3))  # задержка между запросами

        # Запуск всех задач для сбора данных авто
        await asyncio.gather(*car_tasks)

        # Сохранение данных в CSV
        df = pd.DataFrame(cars_data)
        csv_file = 'cars_data.csv'

        try:
            df.to_csv(csv_file, index=False, encoding='utf-8-sig', sep=';')
            print(f"Data successfully saved to {csv_file}")
        except PermissionError as e:
            print(f"Error writing to file: {e}")

# Run the main event loop
asyncio.run(main())
