package Components.Service;

/** A reply: RESP text, optionally followed by raw bytes (the RDB file after FULLRESYNC). */
public class ResponseDto {
    public final String response;
    public final byte[] data;

    public ResponseDto(String response) {
        this(response, null);
    }

    public ResponseDto(String response, byte[] data) {
        this.response = response;
        this.data = data;
    }

    public boolean isError() {
        return response != null && response.startsWith("-");
    }
}
