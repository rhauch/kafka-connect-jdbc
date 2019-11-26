-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  almostEmptyDB
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE almostEmptyDB;

--
-- This table is expected to never have any rows
--
CREATE TABLE emptyTable (
  id MEDIUMINT NOT NULL,
  PRIMARY KEY id
)


-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  testSourceDB
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE testSourceDB;

CREATE TABLE types (
  id MEDIUMINT NOT NULL AUTO_INCREMENT,
  ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  bitcol BIT,
  booleancol BOOLEAN,
  tinyintcol TINYINT(1),
  smallintcol SMALLINT,
  mediumintcol MEDIUMINT,
  intcol INT NOT NULL,
  bigintcol BIGINT,
  floatcol FLOAT,
  doublecol DOUBLE,
  decimalcol DECIMAL(5,2),
  datecol DATE,
  datetimecol DATETIME,
  timestampcol TIMESTAMP,
  timecol TIME,
  charcol CHAR(10),
  varcharcol VARCHAR(10),
  binarycol BINARY(10),
  varbinarycol, VARBINARY(10),
  blobcol BLOB,
  textcol TEXT,
  enumcol ENUM('hearts', 'diamond', 'spades', 'clubs'),
  PRIMARY KEY (id)
)

INSERT INTO types (
  bitcol, booleancol, smallintcol, mediumintcol, intcol, bigintcol,
  floatcol, doublecol, decimalcol,
  datecol, datetimecol, timestampcol, timecol,
  charcol, varcharcol, binarycol, varbinarycol, blobcol, textcol,
  enumcol
) VALUES (
  0, True, 1, 100, 30000, 2000000000, 3223372036854775807L,
  1.2, 1.2, 100.25,
  '2012-08-09', '2012-08-09 00:00:02', '2012-08-09 00:00:02', '00:00:02',
  'abc', 'abc', 'abc', 'abc', 'abc', 'abc',
  'hearts'
)

INSERT INTO types (intcol) VALUE (2000000000)


CREATE TABLE joinA (
  id MEDIUMINT NOT NULL AUTO_INCREMENT,
  ts TIMESTAMP,
  data INT,
  joinid INT,
  PRIMARY KEY (id)
)

CREATE TABLE joinB (
  joinid INT,
  extra INT,
  PRIMARY KEY joinid,
  FOREIGN KEY (joinid) REFERENCES joinA(id)
)

