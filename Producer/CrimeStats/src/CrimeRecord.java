 public class CrimeRecord {
    public String ID;
    public String Date;
    public String IUCR;
    public boolean Arrest;
    public boolean Domestic;
    public String District;
    public String PrimaryDescription;
    public String Month;

    @Override
    public String toString() {
        return "CrimeRecord{" +
                "ID='" + ID + '\'' +
                ", Date='" + Date + '\'' +
                ", IUCR='" + IUCR + '\'' +
                ", Arrest=" + Arrest +
                ", Domestic=" + Domestic +
                ", District='" + District + '\'' +
                ", PrimaryDescription='" + PrimaryDescription + '\'' +
                ", Month='" + Month + '\'' +
                '}';
    }
}