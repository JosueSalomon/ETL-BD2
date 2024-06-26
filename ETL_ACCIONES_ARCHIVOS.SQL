CREATE OR REPLACE PROCEDURE ETL_ACCIONES_ARCHIVOS AS
    V_FECHA_INICIO_BORRADO DATE;
    V_FECHA_INICIO_FAVORITO DATE;
    V_FECHA_FIN DATE := TRUNC(SYSDATE); -- Fecha actual
    V_HORA_INICIO_PROCESO DATE := SYSDATE;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE C##DWH.TBL_ACCIONES_ARCHIVOS';
    
    SELECT MIN(FECHA_BORRADO) 
    INTO V_FECHA_INICIO_BORRADO 
    FROM TBL_ARCHIVOS_BORRADOS;
    
    SELECT MIN(FECHA_GUARDADO) 
    INTO V_FECHA_INICIO_FAVORITO 
    FROM TBL_FAVORITOS;
    
    
    --PRIMERO TABLA BORRADO
    WHILE V_FECHA_INICIO_BORRADO <= V_FECHA_FIN LOOP
    
    DELETE FROM C##DWH.TBL_ACCIONES_ARCHIVOS
    WHERE TRUNC(FECHA_ACCION) = TRUNC(V_FECHA_INICIO_BORRADO);
    
    INSERT INTO C##DWH.TBL_ACCIONES_ARCHIVOS(
        ID_TIPO,
        ID_ARCHIVO,
        ID_USAURIO,
        FECHA_ACCION,
        DESCRIPCION
    )
    SELECT SEQ_ID_ACTIVIDAD.NEXTVAL, ID_ARCHIVO, ID_USUARIO, FECHA_BORRADO, 'BORRADO'
    FROM TBL_ARCHIVOS_BORRADOS
    WHERE TRUNC(FECHA_BORRADO) =TRUNC(V_FECHA_INICIO_BORRADO);
    COMMIT;
             V_FECHA_INICIO_BORRADO := V_FECHA_INICIO_BORRADO + 1;
    END LOOP;
    
    WHILE V_FECHA_INICIO_FAVORITO <= V_FECHA_FIN LOOP
    DELETE FROM C##DWH.TBL_ACCIONES_ARCHIVOS
    WHERE TRUNC(FECHA_ACCION) = TRUNC(V_FECHA_INICIO_BORRADO);
    
    INSERT INTO C##DWH.TBL_ACCIONES_ARCHIVOS(
        ID_TIPO,
        ID_ARCHIVO,
        ID_USAURIO,
        FECHA_ACCION,
        DESCRIPCION
    )
    SELECT SEQ_ID_ACTIVIDAD.NEXTVAL, ID_ARCHIVO, ID_USUARIO, FECHA_GUARDADO, 'FAVORITO'
    FROM TBL_FAVORITOS
    WHERE TRUNC(FECHA_GUARDADO) = TRUNC(V_FECHA_INICIO_FAVORITO);
    COMMIT;
                 V_FECHA_INICIO_FAVORITO := V_FECHA_INICIO_FAVORITO + 1;
    END LOOP;
    
    
   
END ETL_ACCIONES_ARCHIVOS;

BEGIN
    ETL_ACCIONES_ARCHIVOS;
END;

SELECT * FROM TBL_FAVORITOS;
SELECT * FROM TBL_ARCHIVOS_BORRADOS;
SELECT SEQ_ID_ACTIVIDAD.currval, ID_ARCHIVO, ID_USUARIO, FECHA_GUARDADO, 'FAVORITO'
    FROM TBL_FAVORITOS;


DROP SEQUENCE SEQ_ID_ACTIVIDAD;

CREATE SEQUENCE SEQ_ID_ACTIVIDAD
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9999999999999999999999999999
    CACHE 20;


