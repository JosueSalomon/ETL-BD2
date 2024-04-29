--Incremental
CREATE OR REPLACE PROCEDURE ETL_ACTIVIDADES_USUARIOS AS
    V_FECHA_INICIO DATE;
    V_FECHA_FIN DATE := TRUNC(SYSDATE); -- Fecha actual
    V_HORA_INICIO_PROCESO DATE := SYSDATE;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE C##DWH.TBL_ACTIVIDADES_USUARIOS';
    -- Obtener la fecha mínima de la tabla fuente
    SELECT MIN(FECHA_ACTIVIDAD) INTO V_FECHA_INICIO FROM TBL_ACTIVIDADES_USUARIOS;
    
    -- Si no hay datos en la tabla fuente, salir del procedimiento
    IF V_FECHA_INICIO IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('No hay datos para procesar.');
        RETURN;
    END IF;
    
    -- Bucle para cargar los datos desde la fecha mínima hasta la fecha actual
    WHILE V_FECHA_INICIO <= V_FECHA_FIN LOOP
        -- Eliminar los datos de la fecha actual en la tabla de destino
        DELETE FROM C##DWH.TBL_ACTIVIDADES_USUARIOS
        WHERE TRUNC(FEHCA_ACTIVIDAD) = TRUNC(V_FECHA_INICIO);
        
        -- Insertar los datos desde la fecha actual en la tabla de destino
        INSERT INTO C##DWH.TBL_ACTIVIDADES_USUARIOS(
            ID_ACTIVIDAD, 
            TIPO_ACTIVIDAD,  
            FEHCA_ACTIVIDAD, 
            DESCRIPCION, 
            ID_USAURIO 
        )
        SELECT ID_ACTIVIDAD,
               TIPO_ACTIVIDAD,
               FECHA_ACTIVIDAD,
               DESCRIPCION,
               ID_USUARIO
        FROM TBL_ACTIVIDADES_USUARIOS
        WHERE TRUNC(FECHA_ACTIVIDAD) = TRUNC(V_FECHA_INICIO);
        
        -- Confirmar los cambios en la base de datos
        COMMIT;
        
        -- Incrementar la fecha
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
END ETL_ACTIVIDADES_USUARIOS;
/


BEGIN
    ETL_ACTIVIDADES_USUARIOS;
END;

--JOB ETL_ACTIVIDADES_USUARIOS
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
        PROGRAM_NAME => 'PR_ETL_ACTIVIDADES_USUARIOS',
        PROGRAM_TYPE => 'PLSQL_BLOCK',
        PROGRAM_ACTION => 'BEGIN ETL_ACTIVIDADES_USUARIOS; END;',
        ENABLED => TRUE,
        COMMENTS => 'EJECUCION PROGRAMA ETL_ACTIVIDADES_USUARIOS'
    );
END;

BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
        JOB_NAME => 'JOB_ETL_ACTIVIDADES_USUARIOS',
        PROGRAM_NAME => 'PR_ETL_ACTIVIDADES_USUARIOS',
        SCHEDULE_NAME => 'INTERVALO_DIARIO',
        ENABLED => TRUE,
        AUTO_DROP => FALSE,
        COMMENTS => 'EJECUCION DEL JOB ETL_ACTIVIDADES_USUARIOS'
    );
END;


