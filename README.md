# Портфолио с работами Корягина А.Н.

## [/Pharmacy recommendation system][8] - Проект рекомендательной системы для аптечной сети
 - Стек: HDFS, Cassandra, Spark_ML (Word2Vec, ALS, Kmeans);
 - Данные: данные продаж аптечной сети за 11 месяцев
 - Задача: Построение рекомендательной системы, делающей предскзание товара на основе похожих покупателей и похожих товаров (2 метода). А также кластеризация товаров на классы по контексту покупок.

## [/job change of data scientists][7] - Потоковая обработка данных. Прогнозирование трудоустройства.
- Стек: HDFS, Kafka, Cassandra, Spark_ML (LogisticRegression);
- Данные: соревнование - https://www.kaggle.com/arashnic/hr-analytics-job-change-of-data-scientists
- Задача: Прогнозирование на потоке (из Kafka), будет ли кандидат будет работать в компании-нанимателе.

## [/pharmacy turnover forecast][4] - моделирование месячного товарооборота аптеки
- Стек: pandas, numpy, fbprophet/ (https://facebook.github.io/prophet/docs/installation.html#python)
- Задача: С помощью машинного обучения предсказать выручку аптеки на основе данных о ежедневных продажах за 2 предыдущих года с учетом сезонности и праздничных дней.
- Модель: fbp.Prophet, метрика: mean_absolute_error

## [/seasonality of drug sales][6] - проверка гипотезы о наличии сезонности продаж транквилизаторов
- Стек: numpy, pandas, seaborn, scipy
- Данные: с kaggle - https://www.kaggle.com/milanzdravkovic/pharma-sales-data
- Задача: Проверка гипотезы. Есть мнение, что транквилизаторы продаются лучше в декабре. Проверить, что ЛП из группы *N05B - Psycholeptics drugs, Anxiolytic drugs* имеют повышенные продажи в декабре и это статистически значимы.
- Описание: Набор данных построен на основе исходного набора данных, состоящего из 600000 транзакционных данных, собранных за 6 лет (период 2014-2019 гг.). С указанием даты и времени продажи, торговой марки фармацевтического препарата и проданного количества, экспортированных из системы точек продаж на индивидуальном уровне. аптека. Выбранная группа препаратов из набора данных (57 препаратов) классифицируется по категориям системы анатомо-терапевтической химической классификации (АТХ).

## [/prediction_of_cardiovascular_disease][5] - Прогнозирование наличия сердечно-сосудистого заболевания по введенным данным. Бинарная классификация
- Стек:ML: sklearn, pandas, numpy, API: flask
- Данные: соревнование - https://mlbootcamp.ru/ru/round/12/sandbox/
- Задача: предсказать наличиe сердечно-сосудистого заболевания по введенным данным. Бинарная классификация

## [/crawl hh and sj][1] - Парсинг сайтов о работе hh.ru и superjob.ru
- Стек: scrapy, MongoDB
- Задача: Сбор вакансий с сайтов о работе и помещение их в базу данных MongoDB или файл .csv

## [/predictions of apartment prices][3] - предсказание цен на квартиры в Москве
- Стек: sklearn, pandas, numpy
- Данные: с kaggle - https://www.kaggle.com/c/realestatepriceprediction
- Задача: С помощью модели, предсказать цены для квартир из тестового датасета.
- Модель: ExtraTreesRegressor, метрика: r2_score

## [/mysql_b_apteka][2] - последовательный набор sql-запросов для создания MySQL-базы данных
- Стек: MySQL

Данная база данных представляет собой пример организации интернет-аптеки (учебный проект). В базе хранится информация о наличии товаров (stocks) в разных аптеках, а также о заказах товаров (orders), и из статусах. Также в базе хранятся классификация товаров по категориям и по другим параметрам.

---
[1]: https://github.com/koryagin2006/portfolio/tree/main/crawl%20hh%20and%20sj
[2]: https://github.com/koryagin2006/portfolio/tree/main/mysql_b_apteka
[3]: https://github.com/koryagin2006/portfolio/tree/main/predictions%20of%20apartment%20prices
[4]: https:https://github.com/koryagin2006/portfolio/tree/main/pharmacy%20turnover%20forecast
[5]: https://github.com/koryagin2006/prediction_of_cardiovascular_disease
[6]: https://github.com/koryagin2006/portfolio/blob/main/seasonality%20of%20drug%20sales/seasonality_of_drug_sales.ipynb
[7]: https://github.com/koryagin2006/portfolio/blob/main/job%20change%20of%20data%20scientists/project_enrollees.ipynb
[8]: https://github.com/koryagin2006/Pharmacy_recommendation_system/
