SELECT * FROM TBL_GENEROS_X_PRODUCTOS;
CREATE OR REPLACE PROCEDURE ETL_GENEROS_X_PRODUCTOS AS
BEGIN

    EXECUTE IMMEDIATE 'TRUNCATE TABLE C##DWH.TBL_GENEROS_X_PRODUCTOS';

    INSERT INTO C##DWH.TBL_GENEROS_X_PRODUCTOS(
        ID_GENERO_X_PRODUCTO,
        ID_CODIGO_PRDUCTO,
        GENERO
    )
    SELECT SEQ_ID_GENERO_X_PRODUCTO.NEXTVAL,A."codigo_producto", B."nombre"
    FROM tbl_productos@DATABASELINK_MYSQL A
    INNER JOIN tbl_categorias@DATABASELINK_MYSQL B
    ON (A."codigo_categoria" = B."codigo_categoria");
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
END ETL_GENEROS_X_PRODUCTOS;



BEGIN
    ETL_GENEROS_X_PRODUCTOS;
END;

CREATE SEQUENCE SEQ_ID_GENERO_X_PRODUCTO
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9999999999999999999999999999
    CACHE 20;
    
SELECT SEQ_ID_GENERO_X_PRODUCTO.NEXTVAL FROM DUAL;
