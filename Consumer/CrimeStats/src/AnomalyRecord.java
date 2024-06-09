public class AnomalyRecord {
    public String start;
    public String end;
    public String district;
    public long fbiCrimes;
    public long totalCrimes;
    public double percentage;

    @Override
    public String toString() {
        return "AnomalyRecord{" +
                "start='" + start + '\'' +
                ", end='" + end + '\'' +
                ", district='" + district + '\'' +
                ", fbiCrimes=" + fbiCrimes +
                ", totalCrimes=" + totalCrimes +
                ", percentage=" + percentage +
                '}';
    }
}
