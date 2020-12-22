# Портфолио с работами Корягина А.Н.

## [/crawl hh and sj][1] - Парсинг сайтов о работе hh.ru и superjob.ru
- Стек: scrapy, MongoDB
- Задача: Сбор вакансий с сайтов о работе и помещение их в базу данных MongoDB или файл .csv

## [/mysql_b_apteka][2] - последовательный набор sql-запросов для создания MySQL-базы данных
- Стек: MySQL

Данная база данных представляет собой пример организации интернет-аптеки (учебный проект). В базе хранится информация о наличии товаров (stocks) в разных аптеках, а также о заказах товаров (orders), и из статусах. Также в базе хранятся классификация товаров по категориям и по другим параметрам.

## [/predictions of apartment prices][3] - итоговый проект курса "Библиотеки Python для Data Science"
- Стек: sklearn, pandas, numpy
- Данные: с kaggle - https://www.kaggle.com/c/realestatepriceprediction
- Задача: построить модель для предсказания цен на недвижимость (квартиры). С помощью полученной модели, предсказать цены для квартир из тестового датасета.
- Модель: ExtraTreesRegressor, метрика: r2_score

## [/pharmacy turnover forecast][4] - моделирование месячного товарообота аптеки
- Стек: pandas, numpy, fbprophet/ (https://facebook.github.io/prophet/docs/installation.html#python)
- Задача: С помошью машинного обучения предсказать выручку аптеки на основе данных о ежедневных продажах за 2 предыдущих года с учетом сезонности и праздничных дней.
- Модель: fbp.Prophet, метрика: mean_absolute_error

## [/seasonality of drug sales][6] - проверка гипотезы о наличии сезонности продаж транквилизаторов
- Стек: numpy, pandas, seaborn, scipy
- Данные: с kaggle - https://www.kaggle.com/milanzdravkovic/pharma-sales-data
- Задача: Проверка гипотезы. Есть мнение, что транквилизаторы продаются лучше в декабре. Проверить, что ЛП из группы *N05B - Psycholeptics drugs, Anxiolytic drugs* имеют повышенные продажи в декабре и это статистически значимы.
- Описание: Набор данных построен на основе исходного набора данных, состоящего из 600000 транзакционных данных, собранных за 6 лет (период 2014-2019 гг.). С указанием даты и времени продажи, торговой марки фармацевтического препарата и проданного количества, экспортированных из системы точек продаж на индивидуальном уровне. аптека. Выбранная группа препаратов из набора данных (57 препаратов) классифицируется по категориям системы анатомо-терапевтической химической классификации (АТХ).

## [/prediction_of_cardiovascular_disease][5] - Прогноз наличия сердечно-сосудистого заболевания. Бинарная классификация
- Стек:ML: sklearn, pandas, numpy, API: flask
- Данные: соревнование - https://mlbootcamp.ru/ru/round/12/sandbox/
- Задача: предсказать по описанию вакансии является ли она фейком или нет (поле fraudulent). Бинарная классификация

---
[1]: https://github.com/koryagin2006/portfolio/tree/main/crawl%20hh%20and%20sj
[2]: https://github.com/koryagin2006/portfolio/tree/main/mysql_b_apteka
[3]: https://github.com/koryagin2006/portfolio/tree/main/predictions%20of%20apartment%20prices
[4]: https:https://github.com/koryagin2006/portfolio/tree/main/pharmacy%20turnover%20forecast
[5]: https://github.com/koryagin2006/prediction_of_cardiovascular_disease
[6]: https://github.com/koryagin2006/portfolio/blob/main/seasonality%20of%20drug%20sales/seasonality_of_drug_sales.ipynb
