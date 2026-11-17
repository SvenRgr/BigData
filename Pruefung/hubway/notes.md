### Gender

- nirgends beschireben 0, 1, 2 mann frau unbekannt? laut https://www.erikellis.com/2018/12/hubway-capstone-project-user-type.html

  | gender | Bedeutung |
  | --- | --- |
  | 0 | Unbekannt / nicht angegeben |
  | 1 | Männlich |
  | 2 | Weiblich |

### Pipeline

- hubway stations pipline teil wir nicht gebraucht, könnte entfernt werden, habe es gelassen
- vlt für andere KPIs noch
- ebenfalls könnte usertype aus trip data entfernt werden, da für KPIs nicht genutzt, bewusst drin gelassen um auch darüber statistiken machen zu können

### Trip length

- zusatz:
  - über osmr wird wird trip length genauer berchnet also nur durch die Luftlinie

### Datenbereinigung

- bewusst keine kompletten Einträge rausgestrichen, unplausible werte werden immer in jeweilger berchnung ignoriert.
- Bsp.
  - wenn kein Alter eingetragen, werden diese als unbekannt extra aufgelistet
  - Wenn positonen außerhalb von Bosten, werden ignoriert bei distanz mittel berechnung, alter wird aber berücksichtigt

### Weitere Hinweise

- zur formatierten Ausgabe in excel und zum Debugging wurde AI genutzt!

