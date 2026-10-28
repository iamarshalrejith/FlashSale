import phoneImage from "../assets/phone1.jpg";

export default function Hero() {
  return (
    <section className="hero">
      <div className="hero-copy">
        <p className="eyebrow">
          LIMITED STOCK · LIMITED TIME
        </p>

        <h1>
          Big deals.
          <br />
          <span>Lightning speed.</span>
        </h1>

        <p className="subtitle">
          One product. Limited quantity.
        </p>
      </div>

      <div className="product-card">
        <div className="product-glow" />

        <img
          src={phoneImage}
          alt="iPhone 15"
          className="phone-image"
        />

        <span className="product-caption">
          iPhone 15
        </span>
      </div>
    </section>
  );
}