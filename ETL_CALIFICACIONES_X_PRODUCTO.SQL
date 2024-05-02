SELECT * FROM TBL_CALIFICACIONES_X_PRODUCTO;
CREATE OR REPLACE PROCEDURE ETL_CALIFICACIONES_X_PRODUCTO AS
    V_FECHA_INICIO DATE;
    V_FECHA_FIN DATE := TRUNC(SYSDATE); -- Fecha actual
    V_HORA_INICIO_PROCESO DATE := SYSDATE;
BEGIN
    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE C##DWH.TBL_CALIFICACIONES_X_PRODUCTO';

    SELECT MIN("fecha_calificacion")  INTO V_FECHA_INICIO
    FROM tbl_calificaciones@DATABASELINK_MYSQL;

    IF V_FECHA_INICIO IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('No hay datos para procesar.');
        RETURN;
    END IF;

    WHILE V_FECHA_INICIO <= V_FECHA_FIN LOOP
    
     DELETE FROM C##DWH.TBL_CALIFICACIONES_X_PRODUCTO
     WHERE TRUNC(FECHA_CALIFICACION) = TRUNC(V_FECHA_INICIO);
     
     INSERT INTO C##DWH.TBL_CALIFICACIONES_X_PRODUCTO(
        ID_CALIFICACION,
        ID_USUARIO,
        ID_CODIGO_PRDUCTO,
        COMENTARIO,
        VALORACION,
        FECHA_CALIFICACION
     )
     SELECT SEQ_ID_CALIFICACION.NEXTVAL, "codigo_usuario",
        "codigo_producto", "comentario", "valoracion",
        "fecha_calificacion"
        FROM tbl_calificaciones@DATABASELINK_MYSQL A
        WHERE TRUNC("fecha_calificacion") = TRUNC(V_FECHA_INICIO);
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
END ETL_CALIFICACIONES_X_PRODUCTO;

BEGIN 
    ETL_CALIFICACIONES_X_PRODUCTO;
END;


SELECT SEQ_ID_CALIFICACION.CURRVAL, "codigo_usuario",
        "codigo_producto", "comentario", "valoracion",
        "fecha_calificacion"
        FROM tbl_calificaciones@DATABASELINK_MYSQL A
 
 
 
 CREATE SEQUENCE SEQ_ID_CALIFICACION
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9999999999999999999999999999
    CACHE 20;
    
SELECT SEQ_ID_CALIFICACION.NEXTVAL FROM DUAL;