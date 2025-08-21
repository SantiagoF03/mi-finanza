import "./App.css";
import MovimientoCard from "./components/MovimientoCard";
import movimientos from "./data/movimientos.json"; // ← JSON real

// Helper: formato de dinero AR
const formatMoney = (n) =>
  Number(n).toLocaleString("es-AR", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });

// Helper: parsea string con coma o punto a número
const toNumber = (x) => parseFloat(String(x).replace(",", "."));

export default function App() {
  // 1) Separo por tipo (más robusto que confiar en el signo)
  const gastos = movimientos.filter((m) => m.tipo === "Gasto");
  const ingresos = movimientos.filter(
    (m) => m.tipo === "Transferencia in" || m.tipo === "Ingreso"
  );

  // 2) Totales
  const totalGastos = gastos.reduce(
    (acc, m) => acc + Math.abs(toNumber(m.monto)),
    0
  );
  const totalIngresos = ingresos.reduce(
    (acc, m) => acc + toNumber(m.monto),
    0
  );
  const balanceNeto = totalIngresos - totalGastos;

  // 3) Top 3 gastos (sin mutar la lista original)
  const topGastos = gastos
    .slice() // ← clonamos para no mutar "gastos"
    .sort(
      (a, b) => Math.abs(toNumber(b.monto)) - Math.abs(toNumber(a.monto))
    )
    .slice(0, 3);

  return (
    <div className="min-h-screen bg-gray-100 p-6">
      <h1 className="text-2xl font-bold mb-4">Resumen financiero</h1>

      {/* KPIs */}
      <section className="grid md:grid-cols-3 gap-4 mb-6">
        <div className="p-4 bg-white rounded-xl shadow text-left">
          <p className="text-sm text-gray-500">Total gastado</p>
          <p className="text-xl font-bold text-red-600">
            ${formatMoney(totalGastos)}
          </p>
        </div>
        <div className="p-4 bg-white rounded-xl shadow text-left">
          <p className="text-sm text-gray-500">Total ingresado</p>
          <p className="text-xl font-bold text-green-600">
            ${formatMoney(totalIngresos)}
          </p>
        </div>
        <div className="p-4 bg-white rounded-xl shadow text-left">
          <p className="text-sm text-gray-500">Balance neto</p>
          <p
            className={`text-xl font-bold ${
              balanceNeto >= 0 ? "text-green-700" : "text-red-700"
            }`}
          >
            ${formatMoney(balanceNeto)}
          </p>
        </div>
      </section>

      {/* Top 3 gastos */}
      <section className="mb-6">
        <h2 className="text-lg font-semibold mb-2 text-left">Top 3 gastos</h2>
        <ul className="space-y-2">
          {topGastos.map((g, i) => (
            <li
              key={g.id ?? `top-${i}`}
              className="flex items-center justify-between bg-white rounded-lg shadow p-3"
            >
              <span className="truncate">{g.descripcion ?? g.comercio}</span>
              <span className="font-bold text-red-600">
                ${formatMoney(Math.abs(toNumber(g.monto)))}
              </span>
            </li>
          ))}
        </ul>
      </section>

      {/* Lista completa de movimientos */}
      <section>
        <h2 className="text-lg font-semibold mb-2 text-left">Movimientos</h2>
      {filteredMovimientos.map((m, i) => {
  const montoNum = m.tipo === "Gasto" ? Math.abs(parseFloat(m.monto)) : parseFloat(m.monto);
  const comercio = m.descripcion ?? m.comercio;
  const fecha = `${m.fecha}${m.hora ? ` ${m.hora}` : ""}`;

  return (
    <MovimientoCard
      key={m.id ?? i}
      fecha={fecha}
      comercio={comercio}
      monto={montoNum.toLocaleString("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
      tipo={m.tipo}
    />
  );
})}
      </section>
    </div>
  );
}