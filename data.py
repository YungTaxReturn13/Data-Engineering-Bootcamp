import faust


class data(faust.Record, validation=True):
    vendorId: str
    example_value: int
