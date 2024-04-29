CREATE OR REPLACE PROCEDURE ETL_TARJETAS AS
    V_ID_TARJETA TBL_TARJETAS.ID_TARJETA%TYPE;
    V_ID_USUARIO_DUENO TBL_TARJETAS.ID_USUARIO_DUENO%TYPE;
    V_NUMERO_TARJETA TBL_TARJETAS.NUMERO_TARJETA%TYPE;
    V_MES_FECHA_VENCIMIENTO TBL_TARJETAS.MES_FECHA_VENCIMIENTO%TYPE;
    V_ANIO_FECHA_VENCIMIENTO TBL_TARJETAS.ANIO_FECHA_VENCIMIENTO%TYPE;
    V_NOMBRE_LUGAR TBL_LUGARES.NOMBRE_LUGAR%TYPE;
    V_TOTAL_FILAS NUMBER;
    V_FILAS_PROCESADAS NUMBER := 0;
BEGIN
    -- Vaciar la tabla de destino
        EXECUTE IMMEDIATE 'TRUNCATE TABLE C##DWH.TBL_TARJETAS';


    -- Contar el total de filas a procesar
    SELECT COUNT(*) INTO V_TOTAL_FILAS
    FROM TBL_TARJETAS a
    INNER JOIN TBL_LUGARES b ON a.ID_LUGAR = b.ID_LUGAR;

    -- Utilizar un cursor para manejar múltiples filas
    FOR tarjeta_row IN (SELECT a.ID_TARJETA, a.ID_USUARIO_DUENO, a.NUMERO_TARJETA, a.MES_FECHA_VENCIMIENTO, a.ANIO_FECHA_VENCIMIENTO, b.NOMBRE_LUGAR
                        FROM TBL_TARJETAS a
                        INNER JOIN TBL_LUGARES b ON a.ID_LUGAR = b.ID_LUGAR) 
    LOOP
        -- Asignar los valores de la fila actual a las variables
        V_ID_TARJETA := tarjeta_row.ID_TARJETA;
        V_ID_USUARIO_DUENO := tarjeta_row.ID_USUARIO_DUENO;
        V_NUMERO_TARJETA := tarjeta_row.NUMERO_TARJETA;
        V_MES_FECHA_VENCIMIENTO := tarjeta_row.MES_FECHA_VENCIMIENTO;
        V_ANIO_FECHA_VENCIMIENTO := tarjeta_row.ANIO_FECHA_VENCIMIENTO;
        V_NOMBRE_LUGAR := tarjeta_row.NOMBRE_LUGAR;

        -- Insertar los datos en la tabla de destino TBL_TARJETAS
        INSERT INTO C##DWH.TBL_TARJETAS (ID_TARJETA, ID_USAURIO, NUMERO_TARJETA, MES_FECHA_VENCIMIENTO, ANIO_FECHA_VENCIMIENTO, LUGAR)
        VALUES (V_ID_TARJETA, V_ID_USUARIO_DUENO, V_NUMERO_TARJETA, V_MES_FECHA_VENCIMIENTO, V_ANIO_FECHA_VENCIMIENTO, V_NOMBRE_LUGAR);
        commit;
        -- Incrementar el contador de filas procesadas
        V_FILAS_PROCESADAS := V_FILAS_PROCESADAS + 1;
        
        -- Salir del bucle si todas las filas han sido procesadas
        EXIT WHEN V_FILAS_PROCESADAS = V_TOTAL_FILAS;
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
END ETL_TARJETAS;
/

begin
    ETL_TARJETAS;
end;

BEGIN
    DBMS_SCHEDULER.CREATE_SCHEDULE (
        SCHEDULE_NAME => 'INTERVALO_DIARIO',
        START_DATE => TRUNC(SYSDATE),
        REPEAT_INTERVAL => 'FREQ=DAILY; INTERVAL=1',
        COMMENTS => 'EJECUCIÓN DIARIA'
    );
END;
/
BEGIN
    DBMS_SCHEDULER.CREATE_PROGRAM(
        PROGRAM_NAME => 'PR_ETL_TARJETAS',
        PROGRAM_TYPE => 'PLSQL_BLOCK',
        PROGRAM_ACTION => 'BEGIN ETL_TARJETAS; END;',
        ENABLED => TRUE,
        COMMENTS => 'EJECUCION PROGRAMA ETL_TARJETAS'
    );
END;

BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
        JOB_NAME => 'JOB_ETL_TARJETAS',
        PROGRAM_NAME => 'PR_ETL_TARJETAS',
        SCHEDULE_NAME => 'INTERVALO_DIARIO',
        ENABLED => TRUE,
        AUTO_DROP => FALSE,
        COMMENTS => 'EJECUCION DEL JOB _ETL_TARJETAS'
    );
END;



select * from tbl_tarjetas;