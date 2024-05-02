--Incremental
CREATE OR REPLACE PROCEDURE ETL_SUSCRIPCIONES AS
    V_FECHA_INICIO DATE;
    V_FECHA_FIN DATE := TRUNC(SYSDATE); -- Fecha actual
    V_HORA_INICIO_PROCESO DATE := SYSDATE;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE C##DWH.TBL_SUSCRIPCIONES';
    -- Obtener la fecha m�nima de la tabla fuente
    SELECT MIN("fecha_inicio") INTO V_FECHA_INICIO FROM TBL_SUSCRIPCIONES@DATABASELINK_MYSQL;
    
    -- Si no hay datos en la tabla fuente, salir del procedimiento
    IF V_FECHA_INICIO IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('No hay datos para procesar.');
        RETURN;
    END IF;
    
    -- Bucle para cargar los datos desde la fecha m�nima hasta la fecha actual
    WHILE V_FECHA_INICIO <= V_FECHA_FIN LOOP
        -- Eliminar los datos de la fecha actual en la tabla de destino
        DELETE FROM C##DWH.TBL_SUSCRIPCIONES
        WHERE TRUNC(FEHCA_INICIO) = TRUNC(V_FECHA_INICIO);
        
        -- Insertar los datos desde la fecha actual en la tabla de destino
        INSERT INTO C##DWH.TBL_SUSCRIPCIONES(
            ID_SUSCRIPCION, 
            ID_USUARIO,  
            FEHCA_INICIO, 
            FEHCA_FIN
        )
        SELECT "codigo_suscripcion",
               "codigo_usuario",
               "fecha_inicio",
               "fecha_fin"
        FROM TBL_SUSCRIPCIONES@DATABASELINK_MYSQL
        WHERE TRUNC("fecha_inicio") = TRUNC(V_FECHA_INICIO);
        
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
        rollback;
        P_ETL_LOG(
            P_NOMBRE_ETL => $$PLSQL_UNIT,
            P_FECHA_HORA_INICIO => sysdate,
            P_ESTATUS => 'F',
            P_ERROR => SQLCODE || ' - ' || SQLERRM
        );
        
END ETL_SUSCRIPCIONES;
/

SELECT * FROM C##DWH.TBL_SUSCRIPCIONES;
BEGIN
    ETL_SUSCRIPCIONES;
END;

