import PyPDF2

def read_pdf(file_name:str):
    # open the pdf file
    with open(file_name, 'rb') as pdf_file:
        # create a pdf reader object
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        # get the number of pages
        num_pages = len(pdf_reader.pages)
        # initialize an empty string to store the text
        text = ''
        # iterate through the pages
        for i in range(num_pages):
            # get the page
            page = pdf_reader.pages[i]
            # extract the text from the page
            text += page.extract_text()

    return text