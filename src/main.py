import Parser

if __name__ == "__main__":
    crdcparsed = Parser.CSVParser("..\crdc.csv")
#    print crdcparsed.get_header()
#    intstore = InternalDataStore()
#    print intstore.get_header_mapping(crdcparsed.get_header())
    crdcparsed.parse()