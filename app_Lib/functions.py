import os


def is_Reserved_word(Supplements, Reserved_words_source, word):
    Reserved_words = Supplements[Supplements['Reserved words source'] == Reserved_words_source][['Reserved words']]
    is_Reserved_word = True if Reserved_words[Reserved_words['Reserved words'] == word]['Reserved words'].any() == word else False
    return is_Reserved_word

def rename_sheet_reserved_word(sheet_df, Supplements_df, Reserved_words_source, columns):
    for col in columns:
        sheet_df[col] = sheet_df.apply(lambda row: rename_reserved_word(Supplements_df, Reserved_words_source, row[col]), axis=1)
    return sheet_df

def rename_reserved_word(Supplements, Reserved_words_source, word):
    return word + '_' if is_Reserved_word(Supplements, Reserved_words_source, word) else word

def get_file_name(file):
    return os.path.splitext(os.path.basename(file))[0]

def get_stg_tables(STG_tables, source_name):
    return STG_tables.loc[STG_tables['Source system name'] == source_name][['Table name', 'Fallback']].drop_duplicates()

def get_stg_table_columns(STG_tables, source_name, Table_name, with_sk_columns=False):
    STG_tables_df = STG_tables.loc[(STG_tables['Source system name'] == source_name)
                                    & (STG_tables['Table name'] == Table_name)
                                   ].reset_index()

    if not with_sk_columns:
        STG_tables_df = STG_tables_df.loc[(STG_tables_df['Key set name'].isnull())
                                          & (STG_tables_df['Code set name'].isnull())
                                          ].reset_index()

    return STG_tables_df


def single_quotes(string):
    return "'%s'" % string


def list_to_string(list, separator=None, between_single_quotes=0):
    if separator is None:
        prefix = ""
    else:
        prefix = separator
    to_string = prefix.join((single_quotes(str(x)) if between_single_quotes == 1 else str(x)) if x is not None else "" for x in list)

    return to_string
