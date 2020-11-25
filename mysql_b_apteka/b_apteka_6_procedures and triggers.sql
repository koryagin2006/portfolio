USE b_apteka;
-- Процедура
	-- Поиск товара по ШК
DELIMITER $$
CREATE PROCEDURE b_apteka.search_by_code(code BIGINT)
BEGIN
	SELECT
		p.barcode,
		p.name,
		r.name,
		m.name,
		m.country
	FROM
		products p
		JOIN release_forms r ON
			p.release_forms_id = r.id
		JOIN manufacturers m ON
			p.manufacturer_id = m.id
	WHERE
		p.barcode = code;
END$$
DELIMITER ;

	-- проверка
CALL search_by_code(9205875929458);

-- Триггер - запрещает именять остаки товаров на отрицательные значения

	-- предварительно снимем ограничение UNSIGNED c столбца 'value'
ALTER TABLE stocks
MODIFY COLUMN value int(10) DEFAULT NULL COMMENT 'Запас товарной позиции в аптеке';

DELIMITER $$
$$
CREATE TRIGGER check_last_products BEFORE UPDATE ON stocks
FOR EACH ROW 
BEGIN
  IF (NEW.value < 0)
  THEN
	SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Товара не хватает для заказа';
  END IF;
END$$
DELIMITER ;
