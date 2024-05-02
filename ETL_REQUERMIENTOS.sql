
create or replace procedure ETL_REQUERMIENTOS AS
BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE C##DWH.TBL_REQUERMIENTOS';
        
        INSERT INTO C##DWH.TBL_REQUERMIENTOS(
            ID_REQUERMIENTO,
            NOMBRE_REQUERIMIENTO,
            DESCRIPCION
        )
        SELECT "codigo_requerimiento", "nombre_requerimiento","descripcion" 
        FROM tbl_requerimientos@DATABASELINK_MYSQL ;
        commit;
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
        rollback;
END ETL_REQUERMIENTOS;

begin
    ETL_REQUERMIENTOS;
end;

SELECT * FROM tbl_requerimientos@DATABASELINK_MYSQL;
select * from TBL_REQUERMIENTOS;