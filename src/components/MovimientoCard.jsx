function MovimientoCard({ fecha, comercio, monto, tipo }) {
  return (
    <div className="border rounded-xl p-4 shadow-md mb-4 bg-white">
      <div className="flex justify-between items-center">
        <h3 className="font-semibold">{comercio}</h3>
        <span className={`font-bold ${tipo === 'Gasto' ? 'text-red-500' : 'text-green-500'}`}>
          ${monto}
        </span>
      </div>
      <p className="text-sm text-gray-500">{fecha}</p>
    </div>
  );
}

export default MovimientoCard;
