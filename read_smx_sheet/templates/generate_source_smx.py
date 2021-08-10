from read_smx_sheet.Logging_Decorator import Logging_decorator
import pandas as pd


@Logging_decorator
def source_smx(STG_tables, Table_mapping,column_mapping, System, BKEY, BMAP, BMAP_values, Supplements, Core_tables, Data_types,
               source_output_path):
    execfile = source_output_path + '/source_smx.xlsx'
    column_mapping = column_mapping.merge(Table_mapping, on='Mapping name', how='inner')
    with pd.ExcelWriter(execfile) as writer:
        STG_tables.to_excel(writer, sheet_name='STG tables', index=False)
        System.to_excel(writer, sheet_name='System', index=False)
        Data_types.to_excel(writer, sheet_name='Data types', index=False)
        Supplements.to_excel(writer, sheet_name='Supplements', index=False)
        BKEY.to_excel(writer, sheet_name='BKEY', index=False)
        BMAP.to_excel(writer, sheet_name='BMAP', index=False)
        BMAP_values.to_excel(writer, sheet_name='BMAP values', index=False)
        Core_tables.to_excel(writer, sheet_name='Core tables', index=False)
        Table_mapping.to_excel(writer, sheet_name='Table mapping', index=False)
        column_mapping.to_excel(writer, sheet_name='Column mapping', index=False)