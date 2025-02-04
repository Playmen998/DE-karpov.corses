## Автоматизация ETL-процессов

### [Create plugin ETL](https://github.com/Playmen998/DE-karpov.corses/tree/main/ETL%20Airflow/Create%20plugin%20ETL)
Первый файл содержит плагин, включающий класс RickMortyHook. Этот класс использует PostgresHook для создания таблицы в GreenPlum и её заполнения данными. Класс RamLocOperator подключается к API, извлекает данные и передаёт их в RickMortyHook для сохранения. Затем DAG использует этот плагин для записи данных в таблицы.

#### Как работает код?  
1. Подключение к API Rick & Morty  
RamLocOperator делает запрос к https://rickandmortyapi.com/api/location, получает список локаций и передает их в RickMortyHook.

2. Создание и очистка таблицы в GreenPlum  
RickMortyHook подключается к базе данных через PostgresHook, создает таблицу (если её нет) и очищает перед добавлением новых данных.

3. Запись данных в таблицу  
RickMortyHook выполняет INSERT INTO, записывая локации в public.e_lavrushkin_ram_location.

4. DAG Airflow  
DAG запускает процесс ежедневно, вызывая RamLocOperator, который, в свою очередь, использует RickMortyHook.

### [Сhecking update date ETL](https://github.com/Playmen998/DE-karpov.corses/tree/main/ETL%20Airflow/%D0%A1hecking%20update%20date%20ETL)
Этот DAG в Apache Airflow предназначен для выполнения заданий с понедельника по субботу, исключая воскресенье. Основная задача — подключение к базе данных GreenPlum через PostgresHook, извлечение данных из указанной таблицы и сохранение результата в XCom для последующего использования в других задачах.

#### Как работает код?  
1. Расписание:  
DAG запускается ежедневно в 10:00 утра, кроме воскресений (schedule_interval='0 10 * * 1-6').

2. Подключение к GreenPlum:  
Используется PostgresHook для подключения к базе данных GreenPlum.

3. Запрос к таблице:  
Выполняется SQL-запрос, который выбирает данные за текущий день недели.

4. Логирование результата:  
Полученные данные записываются в логи Airflow.

5. Передача данных в XCom:  
Извлечённые данные сохраняются в XCom, что позволяет использовать их в других задачах внутри DAG.
