from __future__ import annotations

from .metadata import get_table_sql

def get_wrds_sas(table_name, schema, wrds_id=None, fpath=None,
                     drop=None, keep=None, fix_cr = False, 
                     col_types=None,
                     fix_missing = False, obs=None, where=None,
                     rename=None, encoding=None, sas_encoding=None):
    
    make_table_data = get_table_sql(table_name=table_name, schema=schema, 
                                    wrds_id=wrds_id, fpath=fpath,
                                    col_types=col_types,
                                    drop=drop, rename=rename, keep=keep)

    col_types = make_table_data["col_types"]
    
    if fix_cr:
        fix_missing = True;
        fix_cr_code = """
            * fix_cr_code;  
            array _char _character_;
            
            do over _char;
                _char = compress(_char, , 'kw');
            end;"""
    else:
        fix_cr_code = ""

    if fpath:
        libname_stmt = "libname %s '%s';" % (schema, fpath)
    else:
        libname_stmt = ""

    if rename:
        rename_str = " rename=(" + rename + ")"
    else:
        rename_str = ""
        
    if not sas_encoding:
        sas_encoding_str=""
    else:
        sas_encoding_str="(encoding='" + sas_encoding + "')"

    if fix_missing or drop or obs or keep or col_types or where:
        
        if obs:
            obs_str = " obs=" + str(obs)
        else:
            obs_str = ""

        if drop:
            drop_str = " drop=" + drop + " "
        else:
            drop_str = ""
        
        if keep:
            keep_str = " keep=" + keep + " "
        else:
            keep_str = ""
            
        if where:
            where_str = "where " + where + ";"
        else:
            where_str = ""
        
        if obs or drop or rename or keep:
            sas_table = table_name + "(" + drop_str + keep_str + \
                                           obs_str + rename_str + ")"
        else:
            sas_table = table_name

        # Cut table name to no more than 32 characters
        # (A SAS limitation)
        new_table = "%s%s" % (schema, table_name)
        new_table = new_table[0:min(len(new_table), 32)]
        
        bigints = [k for k, v in col_types.items() if v == "bigint"]
        
        bigints_str = ""
        if bigints:
            decl = "\n".join([f"length {v}__chr $32;" for v in bigints])
            conv = "\n".join(
                [
                    f"if missing({v}) then {v}__chr = '';"
                    f"else {v}__chr = strip(putn({v}, '20.'));"
                    f"drop {v};"
                    f"rename {v}__chr = {v};"
                    for v in bigints
                ]
            )
            bigints_str = decl + "\n" + conv

        if col_types:
            # ---- everything else that is NOT date/time/timestamp/bigint: blank format ----
            unformat = [
                k for k, v in col_types.items()
                if v not in ["date", "time", "timestamp", "bigint"]
            ]
            unformat_str = " ".join([f"attrib {var} format=;" for var in unformat])
            
            # ---- dates / times / timestamps ----
            dates = [k for k, v in col_types.items() if v == "date"]
            dates_str = " ".join([f"attrib {var} format=YYMMDD10.;" for var in dates])
            
            times = [k for k, v in col_types.items() if v == "time"]
            times_str = " ".join([f"attrib {var} format=TIME8.;" for var in times])
            
            timestamps = [k for k, v in col_types.items() if v == "timestamp"]
            timestamps_str = " ".join([f"attrib {var} format=E8601DT19.;" for var in timestamps])
        else:
            unformat_str = ""
            dates_str = ""
            times_str = ""
            timestamps_str = ""
        
        if fix_missing:
            fix_missing_str = """
                * fix_missing code;
                array allvars _numeric_ ;

                do over allvars;
                  if missing(allvars) then allvars = .;
                end;"""
        else:
            fix_missing_str = ""
        
        sas_code = f"""
            options nosource nonotes;
            {libname_stmt}
            * Fix missing values;
            data {new_table};
                set {schema}.{sas_table}{sas_encoding_str};
                {bigints_str}
                {fix_cr_code}
                {fix_missing_str}
                {where_str}
            run;

            proc datasets lib=work;
                modify {new_table}; 
                    {unformat_str}
                    {dates_str}
                    {times_str}
                    {timestamps_str}
            run;

            proc export data={new_table}(encoding="utf-8") 
                outfile=stdout dbms=csv;
            run;"""
    else:

        sas_code = f"""
            options nosource nonotes;
            {libname_stmt}

            proc export data={schema}.{table_name}({rename_str} 
                              encoding="utf-8") outfile=stdout dbms=csv;
            run;"""
    return sas_code
