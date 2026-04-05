from enum import Enum


class Role(str, Enum):
    ADMIN_GENERAL = "admin_general"
    ADMIN_UCARA = "admin_ucara"
    PROGRAMADOR_ALMACEN = "programador_almacen"
    AUXILIAR_UCARA = "auxiliar_ucara"
    AUXILIAR_ALMACEN = "auxiliar_almacen"
    PROFESSOR_LLAVES = "professor_llaves"
    PROFESSOR_INSUMOS = "professor_insumos"
