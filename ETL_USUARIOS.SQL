--ETL de usuarios
--Volatil
CREATE OR REPLACE PROCEDURE ETL_USUARIOS AS
    V_ID_USUARIO tbl_usuarios.ID_USUARIO%TYPE;
    V_NOMBRE tbl_usuarios.NOMBRE%TYPE;
    V_APELLIDO tbl_usuarios.APELLIDO%TYPE;
    V_CORREO tbl_usuarios.CORREO%TYPE;
    V_CONTRASENA tbl_usuarios.CONTRASENA%TYPE;
    V_NUMERO_TELEFONICO tbl_usuarios.NUMERO_TELEFONICO%TYPE;
    V_PAIS tbl_usuarios.PAIS%TYPE;
    V_GENERO tbl_genero.TIPO_GENERO%TYPE;
    v_total_filas NUMBER;
    v_filas_procesadas NUMBER := 0;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE C##DWH.TBL_USUARIOS';

    SELECT COUNT(*) INTO v_total_filas
    FROM tbl_usuarios a
    INNER JOIN tbl_genero b ON a.id_genero = b.id_genero;

    -- Utilizamos un cursor para manejar múltiples filas
    FOR user_row IN (SELECT a.ID_USUARIO, a.NOMBRE, a.APELLIDO, a.CORREO, a.CONTRASENA, a.NUMERO_TELEFONICO, a.PAIS, b.TIPO_GENERO 
                      FROM tbl_usuarios a
                      INNER JOIN tbl_genero b ON a.id_genero = b.id_genero) 
    LOOP
        -- Asignamos los valores de la fila actual a las variables
        V_ID_USUARIO := user_row.ID_USUARIO;
        V_NOMBRE := user_row.NOMBRE;
        V_APELLIDO := user_row.APELLIDO;
        V_CORREO := user_row.CORREO;
        V_CONTRASENA := user_row.CONTRASENA;
        V_NUMERO_TELEFONICO := user_row.NUMERO_TELEFONICO;
        V_PAIS := user_row.PAIS;
        V_GENERO := user_row.TIPO_GENERO;

        -- Insertamos los datos en la tabla TBL_USUARIOS asociada al usuario C##DWH
        INSERT INTO C##DWH.TBL_USUARIOS  (ID_USUARIO, NOMBRE, APELLIDO, CORREO, CONTRASENA, GENERO, NUMERO_TELEFONICO, PAIS)
        VALUES (V_ID_USUARIO, V_NOMBRE, V_APELLIDO, V_CORREO, V_CONTRASENA, V_GENERO, V_NUMERO_TELEFONICO, V_PAIS);
        
        -- Incrementar el contador de filas procesadas
        v_filas_procesadas := v_filas_procesadas + 1;
        
        -- Salir del bucle si todas las filas han sido procesadas
        EXIT WHEN v_filas_procesadas = v_total_filas;
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
END ETL_USUARIOS;
/

BEGIN
    ETL_USUARIOS;
END;
--JOB ETL_USUARIOS
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
        PROGRAM_NAME => 'PR_ETL_USUARIOS',
        PROGRAM_TYPE => 'PLSQL_BLOCK',
        PROGRAM_ACTION => 'BEGIN ETL_USUARIOS; END;',
        ENABLED => TRUE,
        COMMENTS => 'EJECUCION PROGRAMA ETL_USUARIOS'
    );
END;

BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
        JOB_NAME => 'JOB_ETL_USUARIOS',
        PROGRAM_NAME => 'PR_ETL_USUARIOS',
        SCHEDULE_NAME => 'INTERVALO_DIARIO',
        ENABLED => TRUE,
        AUTO_DROP => FALSE,
        COMMENTS => 'EJECUCION DEL JOB ETL_USUARIOS'
    );
END;



