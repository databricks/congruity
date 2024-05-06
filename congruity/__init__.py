from congruity.json_conversion import to_json_conversion

_monkey_patch_complete = False

def monkey_patch_spark():
    global _monkey_patch_complete
    if _monkey_patch_complete:
        return
    from pyspark.sql.connect.dataframe import DataFrame
    # Register all of the monkey patches here
    DataFrame.toJSON = to_json_conversion


    _monkey_patch_complete = True
    return


monkey_patch_spark()