create or replace PROCEDURE ETL_NOTIFICACIONES AS
    V_FECHA_INICIO DATE;
    V_FECHA_FIN DATE := TRUNC(SYSDATE); -- Fecha actual
    V_HORA_INICIO_PROCESO DATE := SYSDATE;
BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE C##DWH.tbl_notificaciones';

        select MIN(FECHA_ENVIO) INTO V_FECHA_INICIO from tbl_notificaciones;

        IF V_FECHA_INICIO IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('No hay datos para procesar.');
            RETURN;
        END IF;

        WHILE V_FECHA_INICIO <= V_FECHA_FIN LOOP

        DELETE FROM C##DWH.tbl_notificaciones
        WHERE TRUNC(FEHCA_ENVIO) = TRUNC(V_FECHA_INICIO);

        insert into C##DWH.tbl_notificaciones(
            ID_NOTIFICACION,
            ID_USAURIO,
            MENSAJE,
            FEHCA_ENVIO,
            LEIDA
        )
        SELECT 
            ID_NOTIFICACION,
            ID_USUARIO,
            MENSAJE,
            FECHA_ENVIO,
            LEIDA
        FROM tbl_notificaciones
        WHERE TRUNC(FECHA_ENVIO) = TRUNC(V_FECHA_INICIO);

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
        P_ETL_LOG(
            P_NOMBRE_ETL => $$PLSQL_UNIT,
            P_FECHA_HORA_INICIO => sysdate,
            P_ESTATUS => 'F',
            P_ERROR => SQLCODE || ' - ' || SQLERRM
        );
END ETL_NOTIFICACIONES;