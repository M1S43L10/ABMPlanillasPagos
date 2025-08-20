from GUI.gui_mian import GUI_MAIN

dict_datos = {}

dict_datos["__version__"] = "0.0.0"
print(f"__version__ = {dict_datos["__version__"]}")


if __name__ == "__main__":
    config = GUI_MAIN(dict_datos)