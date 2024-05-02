CREATE OR REPLACE PROCEDURE ETL_LIBROS AS
BEGIN
    -- Truncar la tabla de destino
    EXECUTE IMMEDIATE 'TRUNCATE TABLE C##DWH.TBL_LIBROS';
    
    -- Insertar registros desde la tabla de origen a la tabla de destino
    INSERT INTO C##DWH.TBL_LIBROS(
         ID_CODIGO_PRDUCTO,
        EDICION,
        CANTIDAD_PAGINAS,
        IDIOMA,
        EDITORIAL,
        ISBN
    )   
    SELECT A."codigo_producto",
           A."edicion",
           A."cantidad_paginas",
           B."nombre_idioma",
           C."nombre_editorial",
           A."isbn"
    FROM TBL_LIBROS@DATABASELINK_MYSQL A
    INNER JOIN TBL_IDIOMAS@DATABASELINK_MYSQL B
    ON (A."codigo_idioma" = B."codigo_idioma")
    INNER JOIN TBL_EDITORIAL@DATABASELINK_MYSQL C
    ON (A."codigo_editorial" = C."codigo_editorial");
    
    COMMIT;
    
    P_ETL_LOG(
        P_NOMBRE_ETL => $$PLSQL_UNIT,
        P_FECHA_HORA_INICIO => sysdate,
        P_ESTATUS => 'S',
        P_ERROR => ''
    );
    
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('No se encontraron datos.');
        -- Agregar al log de errores
        P_ETL_LOG(
            P_NOMBRE_ETL => $$PLSQL_UNIT,
            P_FECHA_HORA_INICIO => sysdate,
            P_ESTATUS => 'F',
            P_ERROR => SQLCODE || ' - ' || SQLERRM
        );
    

END ETL_LIBROS;

SELECT * FROM TBL_LIBROS;

BEGIN
ETL_LIBROS;
END;