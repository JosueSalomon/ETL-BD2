SELECT * FROM TBL_PRODUCTOS;
SELECT * FROM tbl_aplicaciones@DATABASELINK_MYSQL;

CREATE OR REPLACE PROCEDURE ETL_PRODUCTOS AS 
    V_FECHA_INICIO DATE;
    V_FECHA_FIN DATE := TRUNC(SYSDATE); -- Fecha actual
    V_HORA_INICIO_PROCESO DATE := SYSDATE;
BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE C##DWH.TBL_PRODUCTOS';
        
        
        INSERT INTO C##DWH.TBL_PRODUCTOS(
            ID_CODIGO_PRDUCTO,
            TIPO_PRODUCTO,
            FORMATO,
            COSTO,
            LICENCIA,
            NOMBRE_PRODUCTO,
            DESCRIPCION,
            TAMANIO_BYTES
        )
        SELECT A."codigo_producto",B."tipo_producto",
        C."nombre_formato",A."precio", D."nombre_licencia",
        A."nombre_producto",A."descripcion",A."tamanio_bytes"
        FROM tbl_productos@DATABASELINK_MYSQL A
        INNER JOIN tbl_tipos_productos@DATABASELINK_MYSQL B
        ON(A."codigo_tipo_producto" = B."codigo_tipo_producto")
        INNER JOIN tbl_formatos@DATABASELINK_MYSQL C
        ON(A."codigo_formato" = C."codigo_formato")
        INNER JOIN tbl_licencias@DATABASELINK_MYSQL D
        ON(A."codigo_licencia" = D."codigo_licencia");
        
        COMMIT;
               P_ETL_LOG(
        P_NOMBRE_ETL => $$PLSQL_UNIT,
        P_FECHA_HORA_INICIO => sysdate,
        P_ESTATUS => 'S',
        P_ERROR => ''
    );
    -- Hacer commit para confirmar los cambios en la base de datos
    COMMIT;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('No se encontraron datos.');
        -- Agregar al log de errores
        rollback;
        P_ETL_LOG(
            P_NOMBRE_ETL => $$PLSQL_UNIT,
            P_FECHA_HORA_INICIO => sysdate,
            P_ESTATUS => 'F',
            P_ERROR => SQLCODE || ' - ' || SQLERRM
        );

END ETL_PRODUCTOS;

SELECT A."codigo_producto",B."tipo_producto",
        C."nombre_formato",A."precio", D."nombre_licencia",
        A."nombre_producto",A."descripcion",A."tamanio_bytes"
FROM tbl_productos@DATABASELINK_MYSQL A
INNER JOIN tbl_tipos_productos@DATABASELINK_MYSQL B
ON(A."codigo_tipo_producto" = B."codigo_tipo_producto")
INNER JOIN tbl_formatos@DATABASELINK_MYSQL C
ON(A."codigo_formato" = C."codigo_formato")
INNER JOIN tbl_licencias@DATABASELINK_MYSQL D
ON(A."codigo_licencia" = D."codigo_licencia");


BEGIN
    ETL_PRODUCTOS;
END;