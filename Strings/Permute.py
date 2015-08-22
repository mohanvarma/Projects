def permute(string, stringSoFar):
    if len(string) == 0:
        print stringSoFar
    else:
        subString = string[:-1]
        index = 0
        while index <= len(stringSoFar):
            newString = stringSoFar[:index]+string[-1]+stringSoFar[index:]
            permute (subString, newString)
            index += 1


permute("abc", "")
