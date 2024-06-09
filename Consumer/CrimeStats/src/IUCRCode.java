public class IUCRCode {
    public String IUCR;
    public String PrimaryDescription;
    public String SecondaryDescription;
    public String IndexCode;

    @Override
    public String toString() {
        return "IUCRCode{" +
                "IUCR='" + IUCR + '\'' +
                ", PrimaryDescription='" + PrimaryDescription + '\'' +
                ", SecondaryDescription='" + SecondaryDescription + '\'' +
                ", IndexCode='" + IndexCode + '\'' +
                '}';
    }
}