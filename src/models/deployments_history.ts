import { DataTypes, Model } from 'sequelize';
import { sequelize } from '../core/utils/connections';

interface DeploymentsHistoryInterface extends Model {
    id: number;
    deployment_id: number;
    package_id: number;
    created_at: Date;
}

export const DeploymentsHistory = sequelize.define<DeploymentsHistoryInterface>(
    'DeploymentsHistory',
    {
        id: {
            type: DataTypes.INTEGER({ length: 10 }),
            allowNull: false,
            autoIncrement: true,
            primaryKey: true,
        },
        deployment_id: DataTypes.INTEGER({ length: 10 }),
        package_id: DataTypes.INTEGER({ length: 10 }),
        created_at: DataTypes.DATE,
    },
    {
        tableName: 'deployments_history',
        underscored: true,
        updatedAt: false,
        paranoid: true,
    },
);
