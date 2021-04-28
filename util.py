def build_menu(buttons, n_cols, header_buttons=None, footer_buttons=None):
    """
    Build a menu
    """
    menu = [buttons[i : i + n_cols] for i in range(0, len(buttons), n_cols)]
    if header_buttons:
        menu.insert(0, [header_buttons])
    if footer_buttons:
        menu.append([footer_buttons])
    return menu


pincodes = {
    "pincodes": [
        "560034",
        "560001"
    ]
}



zones = {
    "zones": [
        "BOMMANAHALLI",
        "DASARAHALLI",
        "EAST",
        "MAHADEVAPURA",
        "RR NAGAR",
        "SOUTH",
        "WEST",
        "YELAHANKA",
    ]
}
