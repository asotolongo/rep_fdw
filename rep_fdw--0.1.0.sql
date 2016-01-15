
--

CREATE SCHEMA rep_fdw;
SET search_path = rep_fdw, pg_catalog;




SET search_path = rep_fdw, pg_catalog;


CREATE FUNCTION create_f_table(schema_table text, table_name text, server_name text) RETURNS text
    LANGUAGE plpgsql
    AS $$

DECLARE 
temp_table text := 'CREATE FOREIGN TABLE rep_fdw.f_'|| schema_table||'_'||table_name||' '||'('; 
col record;
mens text;
mensdetail text;
sqlerror text; 
BEGIN

--obtengo las columnas de la tabla pasada por parametro para crear las Foregin TABLE
 FOR col IN    SELECT a.attname,t.typname FROM pg_class c, pg_attribute a, pg_type t ,pg_namespace s WHERE s.oid=c.relnamespace and   c.relname = table_name and s.nspname=schema_table 
    AND a.attnum > 0 AND a.attrelid = c.oid AND a.atttypid = t.oid order by attnum LOOP

      temp_table:=temp_table || col.attname ||' '|| col.typname ||',';
 END LOOP;   
      --quitando la , del final y sustituyendo por )
 temp_table:=substring(temp_table from 1 for length(temp_table)-1) ||') SERVER ' || server_name|| ' OPTIONS (schema_name '''||schema_table||''', table_name '''||table_name||''')' ;

      
 RAISE NOTICE 'Text of F. Table :%',temp_table;
  BEGIN

  EXECUTE temp_table;
   EXCEPTION
    WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS mens = MESSAGE_TEXT,mensdetail = PG_EXCEPTION_DETAIL,sqlerror=RETURNED_SQLSTATE;
      RAISE EXCEPTION 'Error %, %,% ',sqlerror,mens,mensdetail;
      return 'FAIL';

  END;     




RETURN 'OK';

end;
$$;




--
-- TOC entry 215 (class 1255 OID 16699)
-- Name: create_server(text, text, text, text); Type: FUNCTION; Schema: rep_fdw; Owner: postgres
--

CREATE FUNCTION create_server(name_server text, server_ip text, server_port text, db text) RETURNS text
    LANGUAGE plpgsql
    AS $$

DECLARE 
temp_fdw text := 'CREATE SERVER '||name_server||'
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host '''||server_ip||''', port '''||server_port||''', dbname '''||db||''')';
mens text;
mensdetail text;
sqlerror text; 
BEGIN

RAISE NOTICE 'Text of FDW :%',temp_fdw;
  BEGIN

  EXECUTE temp_fdw;
   EXCEPTION
    WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS mens = MESSAGE_TEXT,mensdetail = PG_EXCEPTION_DETAIL,sqlerror=RETURNED_SQLSTATE;
      RAISE EXCEPTION 'Error %, %,% ',sqlerror,mens,mensdetail;
      return 'FAIL';

  END;


return 'OK';

end;
$$;




--
-- TOC entry 221 (class 1255 OID 16701)
-- Name: create_usermap(text, text, text); Type: FUNCTION; Schema: rep_fdw; Owner: postgres
--

CREATE FUNCTION create_usermap(usermap text, pass text, name_server text) RETURNS text
    LANGUAGE plpgsql
    AS $$

DECLARE 
temp_usermap text := 'CREATE USER MAPPING FOR public
        SERVER '||name_server||'
        OPTIONS (user '''||usermap||''', password '''||pass||''')';

mens text;
mensdetail text;
sqlerror text; 
BEGIN

RAISE NOTICE 'Text of usermap :%',temp_usermap;
  BEGIN

  EXECUTE temp_usermap;
   EXCEPTION
    WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS mens = MESSAGE_TEXT,mensdetail = PG_EXCEPTION_DETAIL,sqlerror=RETURNED_SQLSTATE;
      RAISE EXCEPTION 'Error %, %,% ',sqlerror,mens,mensdetail;
      return 'FAIL';

  END;


return 'OK';

end;
$$;




--
-- TOC entry 222 (class 1255 OID 16696)
-- Name: generar_trigger(text); Type: FUNCTION; Schema: rep_fdw; Owner: postgres
--

CREATE FUNCTION generar_trigger(anombretabla text) RETURNS text
    LANGUAGE plpgsql
    AS $_$declare
temp text;
temp2 text;
cadena text;
tabla text;
mensaje text;
mensajedetalle text;
sqlerror text;
BEGIN
  


temp:='CREATE TRIGGER f_tb_' || replace($1, '.', '_');
temp:=temp||' AFTER INSERT OR UPDATE OR DELETE  ON ' || $1;
temp:= temp||' FOR EACH ROW
  EXECUTE PROCEDURE rep_fdw.tr_ftable()';

  
BEGIN
  EXECUTE temp;
  EXCEPTION
    WHEN syntax_error THEN
      GET STACKED DIAGNOSTICS mensaje = MESSAGE_TEXT,mensajedetalle = PG_EXCEPTION_DETAIL,sqlerror=RETURNED_SQLSTATE;
      RAISE EXCEPTION 'Error %, %,% ',sqlerror,mensaje,mensajedetalle;
    WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS mensaje = MESSAGE_TEXT,mensajedetalle = PG_EXCEPTION_DETAIL,sqlerror=RETURNED_SQLSTATE;
      RAISE EXCEPTION 'Error %, %,% ',sqlerror,mensaje,mensajedetalle;

  END;
  

RETURN temp ;


END;$_$;




--
-- TOC entry 212 (class 1255 OID 16714)
-- Name: get_delete(text, json); Type: FUNCTION; Schema: rep_fdw; Owner: postgres
--

CREATE FUNCTION get_delete(text, json) RETURNS text
    LANGUAGE plpgsql
    AS $_$
DECLARE 
tabla alias for $1;
documento  alias  for $2;
campo text;

consulta text :='DELETE FROM ' || tabla || '  where ';
predicado text :='';
BEGIN
--obtengo la concatenacion de los los atributos-valores
FOR campo IN select *	  from json_object_keys(documento)   LOOP
     predicado:=predicado|| campo||'='||replace((documento->campo)::text,'"','''')|| ' AND ' ;
        
    END LOOP;

consulta := consulta ||predicado;
--elimino el AND del final
consulta := substring(consulta from 1 for length(consulta)-4);
return consulta;

END;

$_$;




--
-- TOC entry 218 (class 1255 OID 16697)
-- Name: get_insert(text, json); Type: FUNCTION; Schema: rep_fdw; Owner: postgres
--

CREATE FUNCTION get_insert(text, json) RETURNS text
    LANGUAGE plpgsql
    AS $_$
DECLARE 
tabla alias for $1;
documento  alias  for $2;
campo text;

consulta text :='INSERT INTO ' || tabla ;
atributos text :=' (';
valores text :=' VALUES (';
BEGIN
--obtengo la concatenacion de los los atributos-valores
FOR campo IN select *	from json_object_keys(documento)   LOOP
     atributos:=atributos|| campo||',';
     valores:=valores|| replace((documento->campo)::text,'"','''')|| ',' ;
        
    END LOOP;
--elimino , del final
atributos := substring(atributos from 1 for length(atributos)-1);
valores := substring(valores from 1 for length(valores)-1);
consulta := consulta ||atributos ||')';
consulta := consulta ||valores ||')';

--consulta := substring(consulta from 1 for length(consulta)-4);
return consulta;

END;

$_$;




--
-- TOC entry 219 (class 1255 OID 16698)
-- Name: get_update(text, json, json); Type: FUNCTION; Schema: rep_fdw; Owner: postgres
--

CREATE FUNCTION get_update(text, json, json) RETURNS text
    LANGUAGE plpgsql
    AS $_$
DECLARE 
tabla alias for $1;
documentonuevo  alias  for $2;
documentoantiguo  alias  for $3;
campo text;


consulta text :='UPDATE ' || tabla || ' SET ' ;
atributos text :=' ';
valores text :='';
BEGIN
--obtengo la concatenacion de los valores del set
FOR campo IN select *	from json_object_keys(documentonuevo)   LOOP
     atributos:=atributos|| campo||'='||replace((documentonuevo->campo)::text,'"','''')|| ',';
     
        
    END LOOP;
--elimino , del final
atributos := substring(atributos from 1 for length(atributos)-1);
consulta := consulta ||atributos ||' WHERE ';
atributos:='';
--obtengo la concatenacion de los valores del set
FOR campo IN select *	from json_object_keys(documentoantiguo)   LOOP
     atributos:=atributos|| campo||'='||replace((documentoantiguo->campo)::text,'"','''')|| ' AND ';
     
        
    END LOOP;
--elimino AND del final
atributos := substring(atributos from 1 for length(atributos)-4);
consulta := consulta ||atributos ;
--consulta := substring(consulta from 1 for length(consulta)-4);
return consulta;

END;

$_$;



--
-- TOC entry 223 (class 1255 OID 16702)
-- Name: tr_ftable(); Type: FUNCTION; Schema: rep_fdw; Owner: postgres
--

CREATE FUNCTION tr_ftable() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
query_temp text;


    BEGIN
 
IF TG_OP='INSERT'  THEN
 query_temp := rep_fdw.get_insert('rep_fdw.f_'||TG_TABLE_SCHEMA||'_'||TG_RELNAME,row_to_json(NEW.*));
 RAISE NOTICE '%',query_temp;
 EXECUTE query_temp;
END IF; 
if TG_OP='UPDATE' then
query_temp := rep_fdw.get_update('rep_fdw.f_'||TG_TABLE_SCHEMA||'_'||TG_RELNAME,row_to_json(NEW.*),row_to_json(OLD.*));
  RAISE NOTICE '%',query_temp;
 EXECUTE query_temp;
end if;
if TG_OP='DELETE'  then
query_temp := rep_fdw.get_delete('rep_fdw.f_'||TG_TABLE_SCHEMA||'_'||TG_RELNAME,row_to_json(OLD.*));
  RAISE NOTICE '%',query_temp;
 EXECUTE query_temp;
end if;

RETURN new;

END;$$;







