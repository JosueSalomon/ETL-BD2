SELECT * FROM TBL_COMPRAS;
CREATE OR REPLACE PROCEDURE ETL_COMPRAS AS
    V_FECHA_INICIO DATE;
    V_FECHA_FIN DATE := TRUNC(SYSDATE); -- Fecha actual
    V_HORA_INICIO_PROCESO DATE := SYSDATE;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE C##DWH.TBL_COMPRAS';
    
    SELECT MIN("fecha_compra") INTO V_FECHA_INICIO FROM tbl_compras@DATABASELINK_MYSQL;
    
    IF V_FECHA_INICIO IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('No hay datos para procesar.');
        RETURN;
    END IF;
    
    WHILE V_FECHA_INICIO <= V_FECHA_FIN LOOP
    
    DELETE FROM C##DWH.TBL_COMPRAS
    WHERE TRUNC(FECHA_COMPRA) = TRUNC(V_FECHA_INICIO);
    
    INSERT INTO C##DWH.TBL_COMPRAS(
         ID_COMPRA,
         ID_USUARIO,
         ID_CODIGO_PRDUCTO,
         FECHA_COMPRA,
         MONTO_TOTAL
    )
    SELECT "codigo_compra", "codigo_usuario","codigo_producto",
    "fecha_compra","monto_total"
    FROM tbl_compras@DATABASELINK_MYSQL
    WHERE TRUNC("fecha_compra") = TRUNC(V_FECHA_INICIO);
COMMIT;
         V_FECHA_INICIO := V_FECHA_INICIO + 1;
    END LOOP;
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
        ROLLBACK;
        P_ETL_LOG(
            P_NOMBRE_ETL => $$PLSQL_UNIT,
            P_FECHA_HORA_INICIO => sysdate,
            P_ESTATUS => 'F',
            P_ERROR => SQLCODE || ' - ' || SQLERRM
        );
END ETL_COMPRAS;

SELECT * FROM TBL_COMPRAS;

BEGIN
    ETL_COMPRAS;
END;