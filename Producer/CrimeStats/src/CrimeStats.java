public class CrimeStats {
    public String Month;
    public String District;
    public String PrimaryDescription;
    public long TotalCrimes;
    public long Arrests;
    public long DomesticCrimes;
    public long FBICrimes;

    @Override
    public String toString() {
        return "CrimeStats{" +
                "Month='" + Month + '\'' +
                ", District='" + District + '\'' +
                ", PrimaryDescription='" + PrimaryDescription + '\'' +
                ", TotalCrimes=" + TotalCrimes +
                ", Arrests=" + Arrests +
                ", DomesticCrimes=" + DomesticCrimes +
                ", FBICrimes=" + FBICrimes +
                '}';
    }
}