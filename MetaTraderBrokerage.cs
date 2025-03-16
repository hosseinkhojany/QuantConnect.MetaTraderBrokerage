/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Orders;
using QuantConnect.Packets;
using QuantConnect.Interfaces;
using QuantConnect.Securities;
using System.Collections.Generic;
using System.Drawing.Printing;
using QuantConnect.Logging;
using MT;
using MtApi5;
using System.Linq;
using static System.Runtime.InteropServices.JavaScript.JSType;
using System.Threading;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using MT.Models;
using MtApi;
using MtProxyUI.SharedQC;
using QuantConnect.Data.Market;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using NodaTime;
using QuantConnect.Orders.Fees;
using Order = QuantConnect.Orders.Order;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using Object = System.Object;

namespace QuantConnect.MetaTraderBrokerage
{
    [BrokerageFactory(typeof(MetaTraderBrokerageFactory))]
    public class MetaTraderBrokerage : Brokerage
    {

        private object Locker = new object();
        private SymbolMapper SymbolMapper = new();
        private LiveNodePacket job;
        private IAlgorithm algorithm;

        private IMT metaTrader;
        private MTType mtType;
        private string port = "8222";
        private bool _isInitialized;


        public override bool IsConnected
        {
            get
            {
                bool connected = metaTrader.ConnectionStat() == ConnectionState.Connected;
                Logging.Log.Trace("IsConnected(MetaTrader):"+ connected);
                return connected;
            }
        }

        public MetaTraderBrokerage(LiveNodePacket job, IAlgorithm algorithm)
            : base("MetaTrader Brokerage")
        {
            Logging.Log.Trace("MetaTraderBrokerage(LiveNodePacket job, IAlgorithm algorithm)(MetaTrader):");
            this.job = job;
            this.algorithm = algorithm;
            Initialize();
        }
        public MetaTraderBrokerage() : base("MetaTraderBrokerage")
        {
            Logging.Log.Trace("MetaTraderBrokerage()(MetaTrader):");
        }

        void ParseEnvs() {

            var errors = new List<string>();
            mtType = Read<MTType>(job.BrokerageData, "mt-type", errors);
            Logging.Log.Trace("mtType(MetaTrader):"+ mtType);
            port = Read<string>(job.BrokerageData, "mt-port", errors);
            Logging.Log.Trace("port(MetaTrader):"+ port);

            if (errors.Count != 0)
            {
                throw new Exception(string.Join(System.Environment.NewLine, errors));
            }

        }

        private void Initialize()
        {
            if (_isInitialized)
            {
                return;
            }
            _isInitialized = true;
            ParseEnvs();
            if (mtType == MTType.MT5)
            {
                metaTrader = new MT5(true);
            }
            else
            {
                metaTrader = new MT4(true);
            }
            
            metaTrader.BeginConnect(port);
            Thread.Sleep(3000);
            
            SymbolMapper.setMtType(mtType);

            OrdersStatusChanged += (sender, orderEvents) => OnOrderEvents(orderEvents);
            AccountChanged += (sender, accountEvent) => OnAccountChanged(accountEvent);
            Message += (sender, messageEvent) => OnMessage(messageEvent);

        }

        
        
        #region Brokerage


        private Symbol GetSymbol(string instrument)
        {
            var securityType = SymbolMapper.GetBrokerageSecurityType(instrument);
            return SymbolMapper.GetLeanSymbol(instrument, securityType, Market.Oanda);
        }
        public override List<Order> GetOpenOrders()
        {
            Logging.Log.Trace("GetOpenOrders(MetaTrader):");

            List<Order> orders = new List<Order>();

            foreach (var orderMt in metaTrader.GetOpenOrders())
            {
                Order order;

                // Convert MetaTrader order fields to Lean order fields
                Symbol symbol =
                    Symbol.Create(orderMt.Symbol, SecurityType.Forex,
                        Market.Oanda); // Adjust market and security type as needed
                decimal quantity = (decimal)orderMt.Lots;
                DateTime time = orderMt.OpenTime;
                string tag = orderMt.Comment;
                OrderProperties properties = new OrderProperties(); // Adjust properties as needed

                decimal limitPrice = (decimal)orderMt.OpenPrice;
                decimal stopPrice = (decimal)orderMt.StopLoss;

                switch (orderMt.Operation)
                {
                    case ENUM_ORDER_TYPE.ORDER_TYPE_BUY:
                        order = new MarketOrder();
                        break;

                    case ENUM_ORDER_TYPE.ORDER_TYPE_SELL:
                        order = new MarketOrder(GetSymbol(symbol), -quantity, time, tag, properties); // Negative quantity for sell
                        break;

                    case ENUM_ORDER_TYPE.ORDER_TYPE_BUY_LIMIT:
                        order = new LimitOrder(GetSymbol(symbol), quantity, limitPrice, time, tag, properties);
                        break;

                    case ENUM_ORDER_TYPE.ORDER_TYPE_SELL_LIMIT:
                        order = new LimitOrder(GetSymbol(symbol), -quantity, limitPrice, time, tag,
                            properties); // Negative quantity for sell
                        break;

                    case ENUM_ORDER_TYPE.ORDER_TYPE_BUY_STOP:
                        order = new StopMarketOrder(GetSymbol(symbol), quantity, stopPrice, time, tag, properties);
                        break;

                    case ENUM_ORDER_TYPE.ORDER_TYPE_SELL_STOP:
                        order = new StopMarketOrder(GetSymbol(symbol), -quantity, stopPrice, time, tag,
                            properties); // Negative quantity for sell
                        break;

                    case ENUM_ORDER_TYPE.ORDER_TYPE_BUY_STOP_LIMIT:
                        order = new StopLimitOrder(GetSymbol(symbol), quantity, stopPrice, limitPrice, time, tag, properties);
                        break;

                    case ENUM_ORDER_TYPE.ORDER_TYPE_SELL_STOP_LIMIT:
                        order = new StopLimitOrder(GetSymbol(symbol), -quantity, stopPrice, limitPrice, time, tag,
                            properties); // Negative quantity for sell
                        break;

                    case ENUM_ORDER_TYPE.ORDER_TYPE_CLOSE_BY:
                        // Handle ORDER_TYPE_CLOSE_BY if needed (no direct equivalent in Lean)
                        continue; // Skip this order type for now

                    default:
                        throw new NotSupportedException($"Unsupported order type: {orderMt.Operation}");
                }

                // Set additional fields from MetaTrader order to Lean order
                // order.Id = orderMt.Ticket;
                // order.Price = (decimal)orderMt.OpenPrice;
                // order.Time = orderMt.OpenTime;
                
                order.BrokerId.Add(orderMt.Ticket.ToString());
                order.Status = OrderStatus.None; // Adjust status as needed

                orders.Add(order);
            }

            return orders;
        }

        public override List<Holding> GetAccountHoldings()
        {
            Logging.Log.Trace("GetAccountHoldings(MetaTrader):");

            List<Holding> holders = new List<Holding>();

            foreach (var position in metaTrader.GetOpenPositions())
            {
                Holding holding = new Holding
                {
                    Symbol = GetSymbol(position.Symbol),
                    AveragePrice = (decimal)position.OpenPrice,
                    CurrencySymbol = "$",
                    Quantity = (decimal)position.Lots
                };
                holders.Add(holding);
            }

            return holders;
        }


        public override List<CashAmount> GetCashBalance()
        {
            Logging.Log.Trace("GetCashBalance(MetaTrader):");
            return new List<CashAmount>
            {
                new CashAmount((decimal)metaTrader.AccountBalance(), "USD")
            };
        }

        SendOrderRq GenerateSendOrderRq(Order order, OrderTypeRequest type)
        {
            
            SendOrderRq mtOrder = new SendOrderRq(type, new MqlTradeRequest(), null);
            
            mtOrder.mqlTradeRequest.Symbol = order.Symbol.Value;
            mtOrder.mqlTradeRequest.Volume = (double)order.Quantity;
            if (type == OrderTypeRequest.CREATE)
            {
                mtOrder.mqlTradeRequest.Comment = GenerateUniqueId();
            }

            switch (order.Type)
            {
                case OrderType.Market:
                    mtOrder.mqlTradeRequest.Type = order.Direction == OrderDirection.Buy ? ENUM_ORDER_TYPE.ORDER_TYPE_BUY : ENUM_ORDER_TYPE.ORDER_TYPE_SELL;
                    break;
                case OrderType.Limit:
                    mtOrder.mqlTradeRequest.Type = order.Direction == OrderDirection.Buy ? ENUM_ORDER_TYPE.ORDER_TYPE_BUY_LIMIT : ENUM_ORDER_TYPE.ORDER_TYPE_SELL_LIMIT;
                    mtOrder.mqlTradeRequest.Price = (double)((LimitOrder)order).LimitPrice;
                    break;
                case OrderType.StopMarket:
                    mtOrder.mqlTradeRequest.Type = order.Direction == OrderDirection.Buy ? ENUM_ORDER_TYPE.ORDER_TYPE_BUY_STOP : ENUM_ORDER_TYPE.ORDER_TYPE_SELL_STOP;
                    mtOrder.mqlTradeRequest.Price = (double)((StopMarketOrder)order).StopPrice;
                    break;
                case OrderType.StopLimit:
                    mtOrder.mqlTradeRequest.Type = order.Direction == OrderDirection.Buy ? ENUM_ORDER_TYPE.ORDER_TYPE_BUY_STOP_LIMIT : ENUM_ORDER_TYPE.ORDER_TYPE_SELL_STOP_LIMIT;
                    mtOrder.mqlTradeRequest.Price = (double)((StopLimitOrder)order).StopPrice;
                    mtOrder.mqlTradeRequest.Stoplimit = (double)((StopLimitOrder)order).LimitPrice;
                    break;
                default: 
                    throw new NotSupportedException("The order type " + order.Type + " is not supported.");

            }

            return mtOrder;
        }

        public override bool PlaceOrder(Order order)
        {
            Logging.Log.Trace("PlaceOrder(MetaTrader):"+ order.ToString());
            var orderFee = new OrderFee(new CashAmount());
            SendOrderRq mtOrder = GenerateSendOrderRq(order, OrderTypeRequest.CREATE);
            lock (Locker)
            {
                bool success = metaTrader.SendOrderAsync(mtOrder);
                OnOrderEvent(success
                    ? new OrderEvent(order, DateTime.UtcNow, orderFee, "MetaTrader PlaceOrder Order Event") { Status = OrderStatus.Submitted }
                    : new OrderEvent(order, DateTime.UtcNow, orderFee, "MetaTrader PlaceOrder Order Event") { Status = OrderStatus.Invalid });
                order.BrokerId.Add(mtOrder.mqlTradeRequest.Comment);
            }

            return true;
        }


        public override bool UpdateOrder(Order order)
        {
            Logging.Log.Trace("UpdateOrder(MetaTrader):"+ order.ToString());
            
            if (!order.BrokerId.Any())
            {
                Log.Trace("Metatrader.UpdateOrder(): Unable to UpdateOrder order without BrokerId.");
                return false;
            }
            
            SendOrderRq mtOrder = GenerateSendOrderRq(order, OrderTypeRequest.MODIFY);
            bool result = metaTrader.SendOrderAsync(mtOrder);
            OnOrderEvent(result
                ? new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, "MetaTrader UpdateOrder Order Event") { Status = OrderStatus.UpdateSubmitted }
                : new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, "MetaTrader UpdateOrder Order Event") { Status = OrderStatus.Invalid });

            return result;
        }

        public override bool CancelOrder(Order order)
        {
            Logging.Log.Trace("CancelOrder(MetaTrader):"+ order.ToString());
            
            if (!order.BrokerId.Any())
            {
                Log.Trace("Metatrader.UpdateOrder(): Unable to CancelOrder order without BrokerId.");
                return false;
            }
            
            SendOrderRq mtOrder = GenerateSendOrderRq(order, OrderTypeRequest.CLOSE);
            bool result = metaTrader.SendOrderAsync(mtOrder);
            OnOrderEvent(result
                ? new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, "MetaTrader CancelOrder Order Event") { Status = OrderStatus.Canceled }
                : new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, "MetaTrader CancelOrder Order Event") { Status = OrderStatus.Invalid });
            return result;
        }


        public override void Connect()
        {
            Logging.Log.Trace("Connect(MetaTrader):");
        }


        public override void Disconnect()
        {
            Logging.Log.Trace("Disconnect(MetaTrader):");
            if (metaTrader != null) {
                metaTrader.BeginDisconnect();
            }
        }

        #endregion
        
        public override void Dispose()
        {
        }

        protected static T Read<T>(IReadOnlyDictionary<string, string> brokerageData, string key, ICollection<string> errors) where T : IConvertible
        {
            if (!brokerageData.TryGetValue(key, out var value))
            {
                errors.Add("BrokerageFactory.CreateBrokerage(): Missing key(MetaTrader):" + key);
                return default(T);
            }

            try
            {
                return value.ConvertTo<T>();
            }
            catch (Exception ex)
            {
                errors.Add($"BrokerageFactory.CreateBrokerage(): Error converting key '{key}' with value '{value}'. {ex.Message}");
                return default(T);
            }
        }
        private string GenerateUniqueId()
        {
            var timestamp = DateTime.UtcNow.ToString("yyyyMMddHHmmssfff");
            var random = new Random();
            var salt = random.Next(10000, 99999);
            return $"{timestamp}{salt}";
        }
    }
}
